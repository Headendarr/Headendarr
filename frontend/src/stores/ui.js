import {defineStore} from 'pinia';

const STORAGE_KEY = 'tic_ui_show_help';
const THEME_KEY_PREFIX = 'tic_ui_theme_';
const THEME_LAST_KEY = 'tic_ui_theme_last';
const TIME_FORMAT_KEY_PREFIX = 'tic_ui_time_format_';
const TIME_FORMAT_LAST_KEY = 'tic_ui_time_format_last';

const normalizeTheme = (value) => (value === 'dark' ? 'dark' : 'light');
const normalizeTimeFormat = (value) => (value === '12h' ? '12h' : '24h');

export const useUiStore = defineStore('ui', {
  state: () => ({
    showHelp: localStorage.getItem(STORAGE_KEY) === 'true',
    theme: normalizeTheme(localStorage.getItem(THEME_LAST_KEY)),
    timeFormat: normalizeTimeFormat(localStorage.getItem(TIME_FORMAT_LAST_KEY)),
  }),
  actions: {
    setShowHelp(value) {
      this.showHelp = !!value;
      localStorage.setItem(STORAGE_KEY, this.showHelp ? 'true' : 'false');
    },
    toggleHelp() {
      this.setShowHelp(!this.showHelp);
    },
    loadThemeForUser(username) {
      const key = username ? `${THEME_KEY_PREFIX}${username}` : null;
      const stored = key ? localStorage.getItem(key) : null;
      this.theme = normalizeTheme(stored || localStorage.getItem(THEME_LAST_KEY));
      return this.theme;
    },
    setTheme(theme, username) {
      this.theme = normalizeTheme(theme);
      if (username) {
        localStorage.setItem(`${THEME_KEY_PREFIX}${username}`, this.theme);
      }
      localStorage.setItem(THEME_LAST_KEY, this.theme);
    },
    loadTimeFormatForUser(username) {
      const key = username ? `${TIME_FORMAT_KEY_PREFIX}${username}` : null;
      const stored = key ? localStorage.getItem(key) : null;
      this.timeFormat = normalizeTimeFormat(stored || localStorage.getItem(TIME_FORMAT_LAST_KEY));
      return this.timeFormat;
    },
    setTimeFormat(timeFormat, username) {
      this.timeFormat = normalizeTimeFormat(timeFormat);
      if (username) {
        localStorage.setItem(`${TIME_FORMAT_KEY_PREFIX}${username}`, this.timeFormat);
      }
      localStorage.setItem(TIME_FORMAT_LAST_KEY, this.timeFormat);
    },
  },
});
