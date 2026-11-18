package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/amarnathcjd/gogram/telegram"

	"gpix/pkg/gpmc"
)

type Bot struct {
	tg          *telegram.Client
	gp          *gpmc.Client
	owner       int64
	xfer        *Transfer
	log         *slog.Logger
	ctx         context.Context
	tempDir     string
	queue       *uploadQueue
	botUsername string
	botID       int64
	proxyDesc   string
	autoUpload  bool
}

func New(cfg Config, gp *gpmc.Client, log *slog.Logger) (*Bot, error) {
	if cfg.BotToken == "" || cfg.APIID == 0 || cfg.APIHash == "" {
		return nil, errors.New("bridge: incomplete config (need BotToken, APIID, APIHash)")
	}
	if log == nil {
		log = slog.Default()
	}
	tgCfg := telegram.ClientConfig{
		AppID:    cfg.APIID,
		AppHash:  cfg.APIHash,
		Session:  cfg.SessionFile,
		LogLevel: telegram.LogInfo,
		FloodHandler: func(err error) bool {
			log.Warn("FLOOD_WAIT", "err", err)
			return true
		},
	}
	if cfg.Proxy != nil {
		tgCfg.Proxy = cfg.Proxy
		log.Info("telegram proxy configured",
			"type", cfg.Proxy.Type(),
			"host", cfg.Proxy.GetHost(),
			"port", cfg.Proxy.GetPort(),
		)
	}
	tg, err := connectTelegramWithFallback(tgCfg, cfg.BotToken, log)
	if err != nil {
		return nil, err
	}

	b := &Bot{
		tg:      tg,
		gp:      gp,
		owner:   cfg.OwnerID,
		xfer:    NewTransfer(cfg.TempDir, cfg.MaxConcurrent),
		log:     log,
		tempDir: cfg.TempDir,
		queue:   &uploadQueue{},
	}
	if cfg.Proxy != nil {
		b.proxyDesc = fmt.Sprintf("%s://%s:%d", cfg.Proxy.Type(), cfg.Proxy.GetHost(), cfg.Proxy.GetPort())
	}
	if me, err := tg.GetMe(); err == nil && me != nil {
		b.botUsername = me.Username
		b.botID = me.ID
	}

	tg.On("message:/upload", b.wrap(b.handleUpload))
	tg.On("message:/uploadall", b.wrap(b.handleUploadAll))
	tg.On("message:/queue", b.wrap(b.handleQueue))
	tg.On("message:/clearqueue", b.wrap(b.handleClearQueue))
	tg.On("message:/get", b.wrap(b.handleGet))
	tg.On("message:/list", b.wrap(b.handleList))
	tg.On("message:/info", b.wrap(b.handleInfo))
	tg.On("message:/start", b.wrap(b.handleInfo))
	tg.On(telegram.OnMessage, b.wrap(b.handleCatchMedia))

	return b, nil
}

func (b *Bot) Start(ctx context.Context) error {
	b.ctx = ctx
	go func() {
		<-ctx.Done()
		_ = b.tg.Stop()
	}()
	b.tg.Idle()
	return nil
}

func (b *Bot) Stop() error { return b.tg.Stop() }

func connectTelegramWithFallback(tgCfg telegram.ClientConfig, botToken string, log *slog.Logger) (*telegram.Client, error) {
	tryConnect := func(c telegram.ClientConfig) (*telegram.Client, error) {
		type res struct {
			cl  *telegram.Client
			err error
		}
		ch := make(chan res, 1)
		go func() {
			cl, err := telegram.NewClient(c)
			if err != nil {
				ch <- res{err: fmt.Errorf("bridge: telegram.NewClient: %w", err)}
				return
			}
			if err := cl.LoginBot(botToken); err != nil {
				_ = cl.Stop()
				ch <- res{err: fmt.Errorf("bridge: LoginBot: %w", err)}
				return
			}
			ch <- res{cl: cl}
		}()
		select {
		case r := <-ch:
			return r.cl, r.err
		case <-time.After(15 * time.Second):
			return nil, errors.New("bridge: telegram login timed out after 15s")
		}
	}

	cl, err := tryConnect(tgCfg)
	if err == nil || tgCfg.Proxy == nil {
		return cl, err
	}
	log.Warn("telegram proxy connection failed; retrying direct connection", "err", err)
	directCfg := tgCfg
	directCfg.Proxy = nil
	directCfg.Session = tgCfg.Session + ".direct"
	return tryConnect(directCfg)
}

func (b *Bot) wrap(fn func(*telegram.NewMessage) error) func(*telegram.NewMessage) error {
	return func(m *telegram.NewMessage) (rerr error) {
		if m.SenderID() != b.owner {
			b.log.Debug("ignoring non-owner message", "sender", m.SenderID(), "owner", b.owner)
			return nil
		}
		defer func() {
			if r := recover(); r != nil {
				b.log.Error("handler panic", "recover", r)
				_, _ = m.Reply(fmt.Sprintf("internal error: %v", r))
				rerr = fmt.Errorf("panic: %v", r)
			}
		}()
		if err := fn(m); err != nil {
			b.log.Error("handler error", "err", err)
			return err
		}
		return nil
	}
}
