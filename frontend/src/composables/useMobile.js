import {computed} from 'vue';
import {useQuasar} from 'quasar';

export function useMobile() {
  const $q = useQuasar();

  const isMobile = computed(() => {
    return $q.screen.lt.md || $q.platform.is.mobile;
  });

  return {
    isMobile,
  };
}
