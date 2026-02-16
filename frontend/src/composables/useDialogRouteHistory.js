import {onMounted, onUnmounted, ref, watch} from 'vue';

/**
 * useDialogRouteHistory - Option B (Internal History State)
 *
 * Intercepts the browser "Back" action to close a dialog instead of navigating away.
 * Uses window.history.state to keep the URL perfectly clean.
 */
const STATE_KEY = '_tic_dialog_id';

export const useDialogRouteHistory = ({isOpen, setOpen, canClose, onBlockedClose}) => {
  // Generate a unique ID for this specific dialog instance
  const dialogId = `d${Math.random().toString(36).slice(2, 9)}`;

  // Track if we have pushed a state entry for this dialog
  const isPushed = ref(false);

  // Track if the current closing operation was triggered by a popstate event (Back button)
  const isPopping = ref(false);

  /**
   * Handle the browser popstate event (Back button / Swipe back)
   */
  const handlePopState = (event) => {
    if (!isOpen.value) {
      return;
    }

    // If the active dialog in history state is no longer us, we should close
    if (event.state?.[STATE_KEY] !== dialogId) {
      if (canClose()) {
        isPopping.value = true;
        setOpen(false);
      } else {
        // Blocked: push our state back onto the stack to prevent navigation
        window.history.pushState({[STATE_KEY]: dialogId}, '');
        if (typeof onBlockedClose === 'function') {
          onBlockedClose();
        }
      }
    }
  };

  /**
   * Watch for manual state changes (opening/closing via UI)
   */
  watch(isOpen, (val) => {
    if (val) {
      // Dialog opened: push a history state if we haven't already
      if (!isPushed.value) {
        window.history.pushState({[STATE_KEY]: dialogId}, '');
        isPushed.value = true;
      }
    } else {
      // Dialog closed: if we have a history entry and weren't already popping, remove it
      if (isPushed.value && !isPopping.value) {
        window.history.back();
      }
      isPushed.value = false;
      isPopping.value = false;
    }
  });

  onMounted(() => {
    window.addEventListener('popstate', handlePopState);

    // If the dialog is already open on mount, ensure history state is initialized
    if (isOpen.value && !isPushed.value) {
      window.history.pushState({[STATE_KEY]: dialogId}, '');
      isPushed.value = true;
    }
  });

  onUnmounted(() => {
    window.removeEventListener('popstate', handlePopState);

    // Cleanup: if the component is destroyed while the state is pushed, try to pop it
    if (isPushed.value && !isPopping.value) {
      // Note: This can be tricky during page navigation, but generally safe for SPAs
      window.history.back();
    }
  });
};
