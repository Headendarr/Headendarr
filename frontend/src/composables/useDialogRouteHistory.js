import {getCurrentInstance, ref, watch} from 'vue';
import {useRoute, useRouter} from 'vue-router';

const DIALOG_STACK_QUERY_KEY = 'tic_dialog_stack';

const toStackValue = (value) => {
  if (Array.isArray(value)) {
    return typeof value[0] === 'string' ? value[0] : '';
  }
  return typeof value === 'string' ? value : '';
};

const readDialogStack = (query) => {
  const raw = toStackValue(query?.[DIALOG_STACK_QUERY_KEY]);
  if (!raw) {
    return [];
  }
  return raw.split(',').map((item) => item.trim()).filter(Boolean);
};

const buildQueryWithStack = (query, stack) => {
  const nextQuery = {...query};
  if (stack.length === 0) {
    delete nextQuery[DIALOG_STACK_QUERY_KEY];
    return nextQuery;
  }
  nextQuery[DIALOG_STACK_QUERY_KEY] = stack.join(',');
  return nextQuery;
};

export const useDialogRouteHistory = ({
                                        isOpen,
                                        setOpen,
                                        canClose,
                                        onBlockedClose,
                                      }) => {
  const route = useRoute();
  const router = useRouter();
  const instance = getCurrentInstance();

  const dialogId = `d${instance?.uid ?? Math.random().toString(36).slice(2)}`;
  const syncingFromRoute = ref(false);
  const lastOpenedByPush = ref(false);

  const withSyncGuard = (fn) => {
    syncingFromRoute.value = true;
    try {
      fn();
    } finally {
      setTimeout(() => {
        syncingFromRoute.value = false;
      }, 0);
    }
  };

  const pushDialogState = async () => {
    const stack = readDialogStack(route.query);
    if (stack[stack.length - 1] === dialogId) {
      return;
    }
    const nextStack = stack.filter((id) => id !== dialogId).concat(dialogId);
    lastOpenedByPush.value = true;
    try {
      await router.push({query: buildQueryWithStack(route.query, nextStack)});
    } catch {
      // Ignore redundant navigation errors.
    }
  };

  const removeDialogState = async ({preferBack} = {preferBack: false}) => {
    const stack = readDialogStack(route.query);
    const index = stack.lastIndexOf(dialogId);
    if (index === -1) {
      lastOpenedByPush.value = false;
      return;
    }

    const isTopDialog = index === stack.length - 1;
    const shouldUseBack = preferBack
      && isTopDialog
      && lastOpenedByPush.value
      && typeof window !== 'undefined'
      && window.history.length > 1;

    if (shouldUseBack) {
      lastOpenedByPush.value = false;
      router.back();
      return;
    }

    const nextStack = stack.filter((id) => id !== dialogId);
    lastOpenedByPush.value = false;
    try {
      await router.replace(
        {query: buildQueryWithStack(route.query, nextStack)});
    } catch {
      // Ignore redundant navigation errors.
    }
  };

  const restoreDialogState = async () => {
    const stack = readDialogStack(route.query);
    if (stack.includes(dialogId)) {
      return;
    }
    const nextStack = stack.concat(dialogId);
    try {
      await router.replace(
        {query: buildQueryWithStack(route.query, nextStack)});
    } catch {
      // Ignore redundant navigation errors.
    }
  };

  watch(
    [() => route.path, () => route.query[DIALOG_STACK_QUERY_KEY]],
    async ([nextPath], previous) => {
      const hasPreviousRouteState = Array.isArray(previous);
      const [previousPath] = hasPreviousRouteState ? previous : [];
      const isInRouteStack = readDialogStack(route.query).includes(dialogId);
      const pathChanged = nextPath !== previousPath;

      // If this dialog is mounted in an already-open state, wait for the
      // isOpen watcher to push route state instead of force-closing here.
      if (!hasPreviousRouteState && isOpen.value && !isInRouteStack) {
        return;
      }

      if (isInRouteStack && !isOpen.value) {
        withSyncGuard(() => {
          setOpen(true);
        });
        return;
      }

      if (!isInRouteStack && isOpen.value) {
        if (!canClose()) {
          if (typeof onBlockedClose === 'function') {
            onBlockedClose();
          }
          if (!pathChanged) {
            await restoreDialogState();
          }
          return;
        }

        lastOpenedByPush.value = false;
        withSyncGuard(() => {
          setOpen(false);
        });
      }
    },
    {immediate: true},
  );

  watch(
    isOpen,
    async (nextOpen) => {
      if (syncingFromRoute.value) {
        return;
      }

      if (nextOpen) {
        await pushDialogState();
        return;
      }

      // Use route replacement for close-state sync. Browser/hardware Back
      // still closes dialogs via route pop, while this avoids push->back
      // bounce loops caused by internal dialog model transitions.
      await removeDialogState({preferBack: false});
    },
    {immediate: true},
  );
};
