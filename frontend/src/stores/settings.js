import {defineStore} from 'pinia';
import axios from 'axios';

let activeSettingsRequest = null;

export const useSettingsStore = defineStore('settings', {
  state: () => ({
    settingsData: null,
    runtimeKey: null,
    isLoaded: false,
    loading: false,
    lastFetchedAt: 0,
    lastError: null,
  }),
  getters: {
    plexAvailable(state) {
      if (!state.isLoaded && !state.settingsData) {
        return null;
      }
      return Boolean(state.settingsData?.plex_available);
    },
  },
  actions: {
    async refreshSettings(options = {}) {
      const force = Boolean(options.force);
      const minAgeMs = Number(options.minAgeMs || 0);
      const ageMs = Date.now() - Number(this.lastFetchedAt || 0);

      if (!force && this.isLoaded && this.settingsData && ageMs <
        Math.max(0, minAgeMs)) {
        return this.settingsData;
      }

      if (activeSettingsRequest) {
        return activeSettingsRequest;
      }

      this.loading = true;
      this.lastError = null;
      activeSettingsRequest = axios({
        method: 'get',
        url: '/tic-api/get-settings',
      }).then((response) => {
        const payload = response?.data?.data || {};
        this.settingsData = payload;
        this.runtimeKey = response?.data?.runtime_key ?? null;
        this.isLoaded = true;
        this.lastFetchedAt = Date.now();
        this.lastError = null;
        return this.settingsData;
      }).catch((error) => {
        this.lastError = error;
        throw error;
      }).finally(() => {
        this.loading = false;
        activeSettingsRequest = null;
      });

      return activeSettingsRequest;
    },
  },
});
