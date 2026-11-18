package web

import (
	"context"
	"net"
	"net/http"
	"runtime/debug"
	"time"
)

func (s *Server) isTrustedLoopback(r *http.Request) bool {
	s.mu.RLock()
	trust := s.hooks.TrustLoopback
	s.mu.RUnlock()
	if !trust {
		return false
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (s *Server) requireSession(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.isTrustedLoopback(r) {
			ctx := context.WithValue(r.Context(), ctxKeyUser, s.cfg.Username)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		c, err := r.Cookie(sessionCookieName)
		if err != nil {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		user, err := s.verifySession(c.Value)
		if err != nil {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		ctx := context.WithValue(r.Context(), ctxKeyUser, user)
		next.ServeHTTP(w, r.WithContext(ctx))
	}
}

func (s *Server) requireAPI(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.isTrustedLoopback(r) {
			ctx := context.WithValue(r.Context(), ctxKeyUser, s.cfg.Username)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		c, err := r.Cookie(sessionCookieName)
		if err != nil {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		user, err := s.verifySession(c.Value)
		if err != nil {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		ctx := context.WithValue(r.Context(), ctxKeyUser, user)
		next.ServeHTTP(w, r.WithContext(ctx))
	}
}

func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "SAMEORIGIN")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) logRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		s.log.Debug("http",
			"method", r.Method,
			"path", r.URL.Path,
			"dur_ms", time.Since(start).Milliseconds(),
			"remote", r.RemoteAddr,
		)
	})
}

func (s *Server) recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("panic", "recover", rec, "stack", string(debug.Stack()))
				http.Error(w, "internal error", 500)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
