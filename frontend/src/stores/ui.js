import {defineStore} from 'pinia';

const STORAGE_KEY = 'tic_ui_show_help';

export const useUiStore = defineStore('ui', {
  state: () => ({
    showHelp: localStorage.getItem(STORAGE_KEY) === 'true',
  }),
  actions: {
    setShowHelp(value) {
      this.showHelp = !!value;
      localStorage.setItem(STORAGE_KEY, this.showHelp ? 'true' : 'false');
    },
    toggleHelp() {
      this.setShowHelp(!this.showHelp);
    },
  },
});
