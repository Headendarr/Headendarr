import {defineStore} from 'pinia';
import axios from 'axios';

export const useAuthStore = defineStore('auth', {
  state: () => ({
    isAuthenticated: false,
    appRuntimeKey: null,
    loading: false,
    token: localStorage.getItem('tic_auth_token') || null,
    user: null,
  }),
  actions: {
    setToken(token) {
      this.token = token;
      if (token) {
        localStorage.setItem('tic_auth_token', token);
        axios.defaults.headers.common.Authorization = `Bearer ${token}`;
      }
    },
    clearToken() {
      this.token = null;
      localStorage.removeItem('tic_auth_token');
      delete axios.defaults.headers.common.Authorization;
    },
    async login(username, password) {
      const response = await axios.post('/tic-api/auth/login',
        {username, password});
      if (response.status === 200 && response.data.success) {
        this.setToken(response.data.token);
        this.user = response.data.user;
        this.isAuthenticated = true;
      }
      return response;
    },
    async logout() {
      try {
        await axios.post('/tic-api/auth/logout');
      } catch (error) {
        console.error(error);
      } finally {
        this.isAuthenticated = false;
        this.user = null;
        this.clearToken();
      }
    },
    async checkAuthentication() {
      this.loading = true;
      try {
        if (this.token) {
          axios.defaults.headers.common.Authorization = `Bearer ${this.token}`;
        }
        const response = await axios.get('/tic-api/check-auth',
          {cache: 'no-store'});
        this.isAuthenticated = response.status === 200;
        if (this.isAuthenticated) {
          let payload = await response.data;
          if (this.appRuntimeKey === null) {
            this.appRuntimeKey = payload.runtime_key;
          } else if (this.appRuntimeKey !== payload.runtime_key) {
            console.log('Reload window as backed was restarted');
            location.reload();
          }
          this.user = payload.user || null;
        }
      } catch (error) {
        console.error(error);
        this.isAuthenticated = false;
        this.user = null;
        this.clearToken();
      } finally {
        this.loading = false;
      }
    },
  },
});
