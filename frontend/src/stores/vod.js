import {defineStore} from 'pinia';
import axios from 'axios';

let activeAdminStatusRequest = null;
let activeLibraryStatusRequest = null;

export const useVodStore = defineStore('vod', {
  state: () => ({
    adminStatus: null,
    adminStatusLoaded: false,
    adminStatusLoading: false,
    adminStatusLastFetchedAt: 0,
    adminStatusLastError: null,

    libraryStatus: null,
    libraryStatusLoaded: false,
    libraryStatusLoading: false,
    libraryStatusLastFetchedAt: 0,
    libraryStatusLastError: null,
    libraryStatusUsername: '',
  }),
  getters: {
    adminPageVisible(state) {
      if (!state.adminStatusLoaded && !state.adminStatus) {
        return false;
      }
      return Boolean(state.adminStatus?.show_page);
    },
    libraryPageVisible(state) {
      if (!state.libraryStatusLoaded && !state.libraryStatus) {
        return false;
      }
      return Boolean(state.libraryStatus?.show_page);
    },
  },
  actions: {
    resetAdminStatus() {
      this.adminStatus = null;
      this.adminStatusLoaded = false;
      this.adminStatusLoading = false;
      this.adminStatusLastFetchedAt = 0;
      this.adminStatusLastError = null;
    },
    resetLibraryStatus() {
      this.libraryStatus = null;
      this.libraryStatusLoaded = false;
      this.libraryStatusLoading = false;
      this.libraryStatusLastFetchedAt = 0;
      this.libraryStatusLastError = null;
      this.libraryStatusUsername = '';
    },
    resetAll() {
      this.resetAdminStatus();
      this.resetLibraryStatus();
    },
    async refreshAdminStatus(options = {}) {
      const force = Boolean(options.force);
      const minAgeMs = Number(options.minAgeMs || 0);
      const ageMs = Date.now() - Number(this.adminStatusLastFetchedAt || 0);

      if (!force && this.adminStatusLoaded && this.adminStatus && ageMs <
        Math.max(0, minAgeMs)) {
        return this.adminStatus;
      }

      if (activeAdminStatusRequest) {
        return activeAdminStatusRequest;
      }

      this.adminStatusLoading = true;
      this.adminStatusLastError = null;
      activeAdminStatusRequest = axios.get('/tic-api/vod/status').
        then((response) => {
          this.adminStatus = response?.data?.data || {};
          this.adminStatusLoaded = true;
          this.adminStatusLastFetchedAt = Date.now();
          this.adminStatusLastError = null;
          return this.adminStatus;
        }).
        catch((error) => {
          this.adminStatusLastError = error;
          this.adminStatus = null;
          this.adminStatusLoaded = true;
          throw error;
        }).
        finally(() => {
          this.adminStatusLoading = false;
          activeAdminStatusRequest = null;
        });

      return activeAdminStatusRequest;
    },
    async refreshLibraryStatus(username, options = {}) {
      const currentUsername = String(username || '').trim();
      const force = Boolean(options.force);
      const minAgeMs = Number(options.minAgeMs || 0);
      const ageMs = Date.now() - Number(this.libraryStatusLastFetchedAt || 0);
      const usernameMatches = currentUsername && this.libraryStatusUsername ===
        currentUsername;

      if (
        !force &&
        usernameMatches &&
        this.libraryStatusLoaded &&
        this.libraryStatus &&
        ageMs < Math.max(0, minAgeMs)
      ) {
        return this.libraryStatus;
      }

      if (activeLibraryStatusRequest) {
        return activeLibraryStatusRequest;
      }

      this.libraryStatusLoading = true;
      this.libraryStatusLastError = null;
      activeLibraryStatusRequest = axios.get('/tic-api/library/status').
        then((response) => {
          this.libraryStatus = response?.data?.data || {};
          this.libraryStatusLoaded = true;
          this.libraryStatusLastFetchedAt = Date.now();
          this.libraryStatusLastError = null;
          this.libraryStatusUsername = currentUsername;
          return this.libraryStatus;
        }).
        catch((error) => {
          this.libraryStatusLastError = error;
          this.libraryStatus = null;
          this.libraryStatusLoaded = true;
          this.libraryStatusUsername = currentUsername;
          throw error;
        }).
        finally(() => {
          this.libraryStatusLoading = false;
          activeLibraryStatusRequest = null;
        });

      return activeLibraryStatusRequest;
    },
  },
});
