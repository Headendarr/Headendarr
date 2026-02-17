import {defineStore} from 'pinia';
import axios from 'axios';

const AUTH_CHECK_TTL_MS = 15000;
const AUTH_REFRESH_WINDOW_MS = 120000;
let refreshPromise = null;

function parseStoredUser() {
  const raw = localStorage.getItem('tic_auth_user');
  if (!raw) {
    return null;
  }
  try {
    return JSON.parse(raw);
  } catch {
    localStorage.removeItem('tic_auth_user');
    return null;
  }
}

function parseExpiryEpochMs(value) {
  if (!value) {
    return null;
  }
  const millis = Date.parse(value);
  if (Number.isNaN(millis)) {
    return null;
  }
  return millis;
}

export const useAuthStore = defineStore('auth', {
  state: () => ({
    isAuthenticated: Boolean(localStorage.getItem('tic_auth_token')),
    appRuntimeKey: null,
    loading: false,
    token: localStorage.getItem('tic_auth_token') || null,
    tokenExpiresAt: localStorage.getItem('tic_auth_expires_at') || null,
    lastAuthCheckAt: 0,
    user: parseStoredUser(),
  }),
  actions: {
    setUser(user) {
      this.user = user || null;
      if (this.user) {
        localStorage.setItem('tic_auth_user', JSON.stringify(this.user));
      } else {
        localStorage.removeItem('tic_auth_user');
      }
    },
    setSession(token, sessionExpiresAt, user = null) {
      this.setToken(token, sessionExpiresAt);
      this.setUser(user);
      this.isAuthenticated = Boolean(token);
      this.lastAuthCheckAt = Date.now();
    },
    setToken(token, sessionExpiresAt = null) {
      this.token = token;
      this.tokenExpiresAt = sessionExpiresAt || null;
      if (token) {
        localStorage.setItem('tic_auth_token', token);
        if (sessionExpiresAt) {
          localStorage.setItem('tic_auth_expires_at', sessionExpiresAt);
        } else {
          localStorage.removeItem('tic_auth_expires_at');
        }
        axios.defaults.headers.common.Authorization = `Bearer ${token}`;
      }
    },
    clearToken() {
      this.token = null;
      this.tokenExpiresAt = null;
      localStorage.removeItem('tic_auth_token');
      localStorage.removeItem('tic_auth_expires_at');
      delete axios.defaults.headers.common.Authorization;
    },
    clearAuthState() {
      this.isAuthenticated = false;
      this.setUser(null);
      this.lastAuthCheckAt = 0;
      this.clearToken();
    },
    isTokenNearExpiry(windowMs = AUTH_REFRESH_WINDOW_MS) {
      const expiryMs = parseExpiryEpochMs(this.tokenExpiresAt);
      if (!expiryMs) {
        return true;
      }
      return expiryMs - Date.now() <= windowMs;
    },
    async login(username, password) {
      const response = await axios.post('/tic-api/auth/login', {username, password});
      if (response.status === 200 && response.data.success) {
        this.setSession(response.data.token, response.data.session_expires_at || null, response.data.user || null);
      }
      return response;
    },
    async logout() {
      try {
        await axios.post('/tic-api/auth/logout');
      } catch (error) {
        console.error(error);
      } finally {
        this.clearAuthState();
      }
    },
    async refreshSession() {
      if (!this.token) {
        throw new Error('No auth token');
      }
      if (refreshPromise) {
        return refreshPromise;
      }
      refreshPromise = (async () => {
        const response = await axios.post('/tic-api/auth/refresh', {});
        if (!(response.status === 200 && response.data.success && response.data.token)) {
          throw new Error('Session refresh failed');
        }
        this.setSession(response.data.token, response.data.session_expires_at || null, response.data.user || null);
        return true;
      })();
      try {
        return await refreshPromise;
      } finally {
        refreshPromise = null;
      }
    },
    async checkAuthentication(options = {}) {
      const force = !!options.force;
      if (!force && this.isAuthenticated && this.token && this.isTokenNearExpiry()) {
        try {
          await this.refreshSession();
          return true;
        } catch (error) {
          console.error('Session refresh before auth check failed:', error);
        }
      }
      if (!force && this.isAuthenticated && this.token && !this.isTokenNearExpiry()) {
        const now = Date.now();
        if (now - this.lastAuthCheckAt < AUTH_CHECK_TTL_MS) {
          return true;
        }
      }
      this.loading = true;
      try {
        if (this.token) {
          axios.defaults.headers.common.Authorization = `Bearer ${this.token}`;
        }
        const response = await axios.get('/tic-api/check-auth', {cache: 'no-store'});
        this.isAuthenticated = response.status === 200;
        if (this.isAuthenticated) {
          let payload = await response.data;
          this.lastAuthCheckAt = Date.now();
          if (this.appRuntimeKey === null) {
            this.appRuntimeKey = payload.runtime_key;
          } else if (this.appRuntimeKey !== payload.runtime_key) {
            console.log('Reload window as backed was restarted');
            location.reload();
          }
          this.tokenExpiresAt = payload.session_expires_at || this.tokenExpiresAt;
          if (this.tokenExpiresAt) {
            localStorage.setItem('tic_auth_expires_at', this.tokenExpiresAt);
          } else {
            localStorage.removeItem('tic_auth_expires_at');
          }
          this.setUser(payload.user || null);
        }
        return this.isAuthenticated;
      } catch (error) {
        console.error(error);
        const status = error?.response?.status;
        if (status === 401 || status === 403) {
          this.clearAuthState();
        }
        return false;
      } finally {
        this.loading = false;
      }
    },
  },
});
