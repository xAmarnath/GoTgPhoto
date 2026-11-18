package bridge

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/amarnathcjd/gogram/telegram"

	"gpix/pkg/disguise"
	"gpix/pkg/gpmc"
)

type QueuedItemDTO struct {
	ChatID    int64  `json:"chatId"`
	MessageID int32  `json:"messageId"`
	Name      string `json:"name"`
}

type Status struct {
	Connected       bool            `json:"connected"`
	BotUsername     string          `json:"botUsername,omitempty"`
	BotID           int64           `json:"botId,omitempty"`
	OwnerID         int64           `json:"ownerId"`
	Proxy           string          `json:"proxy,omitempty"`
	AutoUpload      bool            `json:"autoUpload"`
	ActiveTransfers int             `json:"activeTransfers"`
	MaxConcurrent   int             `json:"maxConcurrent"`
	Queue           []QueuedItemDTO `json:"queue"`
}

var autoUploadFlag atomic.Bool

func (b *Bot) SetAutoUpload(v bool) {
	if b == nil {
		return
	}
	b.autoUpload = v
	autoUploadFlag.Store(v)
}

func (b *Bot) AutoUpload() bool {
	if b == nil {
		return false
	}
	return autoUploadFlag.Load() || b.autoUpload
}

func (b *Bot) SetGPClient(gp *gpmc.Client) {
	if b != nil {
		b.gp = gp
	}
}

func (b *Bot) StatusSnapshot() Status {
	if b == nil {
		return Status{Queue: []QueuedItemDTO{}}
	}
	snap := b.queue.Snapshot()
	q := make([]QueuedItemDTO, len(snap))
	for i, it := range snap {
		q[i] = QueuedItemDTO{
			ChatID:    it.ChatID,
			MessageID: it.MessageID,
			Name:      it.Name,
		}
	}
	return Status{
		Connected:       b.tg != nil && b.tg.IsConnected(),
		BotUsername:     b.botUsername,
		BotID:           b.botID,
		OwnerID:         b.owner,
		Proxy:           b.proxyDesc,
		AutoUpload:      b.AutoUpload(),
		ActiveTransfers: len(b.xfer.Sem),
		MaxConcurrent:   cap(b.xfer.Sem),
		Queue:           q,
	}
}

func (b *Bot) ClearQueueItems() int {
	if b == nil {
		return 0
	}
	return b.queue.Clear()
}

func (b *Bot) DrainQueueToGP(ctx context.Context, limit int, albumKey string, quality gpmc.Quality, passphrase string, progress func(string)) ([]string, error) {
	if b == nil || b.tg == nil {
		return nil, errors.New("telegram bot is not connected")
	}
	items := b.queue.DrainN(limit)
	if len(items) == 0 {
		return nil, nil
	}
	if passphrase == "" {
		passphrase = os.Getenv("GPIX_ENC_PASSPHRASE")
	}
	results := make([]string, 0, len(items))
	for i, it := range items {
		if ctx.Err() != nil {
			break
		}
		if progress != nil {
			progress(fmt.Sprintf("[%d/%d] Fetching %s from Telegram…", i+1, len(items), it.Name))
		}
		parent, err := b.fetchMessageByID(ctx, it.ChatID, it.MessageID)
		if err != nil {
			results = append(results, fmt.Sprintf("%s: error (%v)", it.Name, err))
			continue
		}
		key, name, err := b.transferMessageToGP(ctx, parent, albumKey, quality, passphrase, func(stage string, done, total int64) {
			if progress != nil {
				progress(fmt.Sprintf("[%d/%d] %s · %s (%s)", i+1, len(items), it.Name, stage, fmtPct(done, total)))
			}
		})
		if err != nil {
			results = append(results, fmt.Sprintf("%s: error (%v)", it.Name, err))
			continue
		}
		results = append(results, fmt.Sprintf("%s -> %s", name, key))
	}
	return results, nil
}

// ImportFromSpec imports media directly from a Telegram link or chat:msgId spec into Google Photos.
// Supported formats:
//   - https://t.me/c/<channel_id>/<msg_id> (or /<topic>/<msg_id>)
//   - https://t.me/<username>/<msg_id>
//   - <chat_id>:<msg_id>
//   - <msg_id> (looks up in owner's DM chat with the bot)
func (b *Bot) ImportFromSpec(ctx context.Context, spec string, albumKey string, quality gpmc.Quality, passphrase string, progress func(stage string, done, total int64)) (string, string, error) {
	if b == nil || b.tg == nil {
		return "", "", errors.New("telegram bot is not connected")
	}
	peer, msgID, err := b.parseTelegramSpec(spec)
	if err != nil {
		return "", "", err
	}
	if progress != nil {
		progress("fetching Telegram message", 0, 0)
	}
	msgs, err := b.tg.GetMessages(peer, &telegram.SearchOption{
		IDs: &telegram.InputMessageID{ID: msgID},
	})
	if err != nil {
		return "", "", fmt.Errorf("telegram GetMessages: %w", err)
	}
	if len(msgs) == 0 {
		return "", "", fmt.Errorf("telegram message %d not found (ensure the bot has access or forward the file to @%s)", msgID, b.botUsername)
	}
	msg := &msgs[0]
	if !msg.IsMedia() {
		return "", "", errors.New("the specified Telegram message does not contain media")
	}
	return b.transferMessageToGP(ctx, msg, albumKey, quality, passphrase, progress)
}

func (b *Bot) transferMessageToGP(ctx context.Context, parent *telegram.NewMessage, albumKey string, quality gpmc.Quality, passphrase string, progress func(stage string, done, total int64)) (string, string, error) {
	release, err := b.xfer.Acquire(ctx)
	if err != nil {
		return "", "", err
	}
	defer release()

	f, cleanup, err := b.xfer.Temp("gpix-tgdirect-")
	if err != nil {
		return "", "", err
	}
	defer cleanup()
	_ = f.Close()

	declaredName := tgMediaName(parent)
	if progress != nil {
		progress("downloading from Telegram", 0, parent.File.Size)
	}
	dlPath, err := parent.Download(&telegram.DownloadOptions{
		FileName: f.Name(),
		Threads:  4,
	})
	if err != nil {
		return "", "", fmt.Errorf("telegram download: %w", err)
	}

	uploadPath := dlPath
	commitName := declaredName
	if head, err := readHead(dlPath, 512); err == nil && disguise.ShouldWrap("", declaredName, head) {
		if progress != nil {
			if passphrase != "" {
				progress("encrypting & wrapping as MP4", 0, 0)
			} else {
				progress("wrapping as MP4", 0, 0)
			}
		}
		wrappedPath, werr := wrapTGFile(b.xfer.TempDir, dlPath, declaredName, passphrase)
		if werr != nil {
			return "", "", fmt.Errorf("wrap: %w", werr)
		}
		defer os.Remove(wrappedPath)
		uploadPath = wrappedPath
		commitName = declaredName + ".mp4"
	}

	res, err := b.gp.UploadFileWithProgress(ctx, uploadPath, gpmc.UploadOpts{
		Quality:      quality,
		OverrideName: commitName,
	}, func(ev gpmc.UploadEvent) {
		if progress != nil {
			progress("google photos "+ev.Stage.String(), ev.BytesDone, ev.BytesTotal)
		}
	})
	if err != nil {
		return "", "", fmt.Errorf("google photos upload: %w", err)
	}

	if albumKey != "" && res.MediaKey != "" {
		_ = b.gp.AddMediaToAlbum(ctx, albumKey, []string{res.MediaKey})
		if store := b.gp.AlbumStore(); store != nil {
			_ = store.AddMembers(ctx, albumKey, []string{res.MediaKey})
		}
	}
	return res.MediaKey, declaredName, nil
}

func (b *Bot) SendToOwner(ctx context.Context, mediaKey, passphrase string, progress func(string)) error {
	if b == nil || b.tg == nil {
		return errors.New("telegram bot is not connected")
	}
	if b.owner == 0 {
		return errors.New("TG_OWNER_ID is not configured")
	}

	release, err := b.xfer.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()

	if progress != nil {
		progress("resolving Google Photos URL")
	}
	orig, _, err := b.gp.GetDownloadURL(ctx, mediaKey)
	if err != nil {
		return err
	}
	if orig == "" {
		return fmt.Errorf("no download URL for %s", mediaKey)
	}

	f, cleanup, err := b.xfer.Temp("gpix-tgsend-")
	if err != nil {
		return err
	}
	defer cleanup()

	mimeType, err := httpGetToFileWithProgress(ctx, b.gp.HTTPClient(), orig, f, func(done, total int64) {
		if progress != nil {
			progress("downloading from Google Photos " + fmtPct(done, total))
		}
	})
	_ = f.Close()
	if err != nil {
		return err
	}

	sendPath := f.Name()
	sendName := suggestFileName(orig, mimeType)
	sendMime := mimeType
	if it, ok, _ := b.gp.GetItem(ctx, mediaKey); ok && it.Filename != "" {
		sendName = it.Filename
	}

	if head, err := readHead(sendPath, 8192); err == nil && disguise.LooksDisguised(head) {
		if pw := passphrase; pw == "" {
			passphrase = os.Getenv("GPIX_ENC_PASSPHRASE")
		}
		if disguise.LooksEncrypted(head) && passphrase == "" {
			return disguise.ErrEncrypted
		}
		extractedPath, origName, err := unwrapTGFile(b.xfer.TempDir, sendPath, passphrase)
		if err != nil {
			return err
		}
		defer os.Remove(extractedPath)
		sendPath = extractedPath
		sendName = origName
		sendMime = disguise.MIMEForFilename(origName)
	}

	if progress != nil {
		progress("uploading to Telegram DM")
	}
	mode := ResolveSendMode(sendMime)
	opts := &telegram.MediaOptions{
		Caption:       fmt.Sprintf("%s\nkey: %s", sendName, mediaKey),
		MimeType:      sendMime,
		FileName:      sendName,
		ForceDocument: mode == SendAsDocument,
		Upload:        &telegram.UploadOptions{Threads: 4},
	}
	if mode == SendAsVideo {
		opts.Attributes = []telegram.DocumentAttribute{&telegram.DocumentAttributeVideo{}}
	}
	_, err = b.tg.SendMedia(b.owner, sendPath, opts)
	return err
}

func (b *Bot) parseTelegramSpec(spec string) (any, int32, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, 0, errors.New("empty telegram link or message id")
	}

	if strings.HasPrefix(spec, "http://") || strings.HasPrefix(spec, "https://") || strings.HasPrefix(spec, "t.me/") {
		if strings.HasPrefix(spec, "t.me/") {
			spec = "https://" + spec
		}
		u, err := url.Parse(spec)
		if err != nil {
			return nil, 0, fmt.Errorf("invalid link: %w", err)
		}
		parts := strings.FieldsFunc(strings.Trim(u.Path, "/"), func(r rune) bool { return r == '/' })
		if len(parts) >= 3 && parts[0] == "c" {
			chNum, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return nil, 0, fmt.Errorf("invalid channel id %q", parts[1])
			}
			msgID, err := strconv.ParseInt(parts[len(parts)-1], 10, 32)
			if err != nil || msgID <= 0 {
				return nil, 0, fmt.Errorf("invalid message id %q", parts[len(parts)-1])
			}
			peerID, _ := strconv.ParseInt(fmt.Sprintf("-100%d", chNum), 10, 64)
			return peerID, int32(msgID), nil
		}
		if len(parts) >= 2 {
			username := parts[0]
			msgID, err := strconv.ParseInt(parts[len(parts)-1], 10, 32)
			if err != nil || msgID <= 0 {
				return nil, 0, fmt.Errorf("invalid message id %q", parts[len(parts)-1])
			}
			return username, int32(msgID), nil
		}
		return nil, 0, errors.New("unsupported t.me link format (expected https://t.me/<chat>/<msg_id> or https://t.me/c/<id>/<msg_id>)")
	}

	if chatPart, msgPart, ok := strings.Cut(spec, ":"); ok {
		msgID, err := strconv.ParseInt(strings.TrimSpace(msgPart), 10, 32)
		if err != nil || msgID <= 0 {
			return nil, 0, fmt.Errorf("invalid message id %q", msgPart)
		}
		chatPart = strings.TrimSpace(chatPart)
		if cid, cerr := strconv.ParseInt(chatPart, 10, 64); cerr == nil {
			return cid, int32(msgID), nil
		}
		return strings.TrimPrefix(chatPart, "@"), int32(msgID), nil
	}

	msgID, err := strconv.ParseInt(spec, 10, 32)
	if err != nil || msgID <= 0 {
		return nil, 0, fmt.Errorf("invalid message id %q", spec)
	}
	if b.owner == 0 {
		return nil, 0, errors.New("TG_OWNER_ID not set; pass <chat_id>:<msg_id> or a t.me link")
	}
	return b.owner, int32(msgID), nil
}
