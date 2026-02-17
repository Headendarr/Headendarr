import {route} from 'quasar/wrappers';
import {createRouter, createMemoryHistory, createWebHistory, createWebHashHistory} from 'vue-router';
import {useAuthStore} from 'stores/auth';
import routes from './routes';

/*
 * If not building with SSR mode, you can
 * directly export the Router instantiation;
 *
 * The function below can be async too; either use
 * async/await or return a Promise which resolves
 * with the Router instance.
 */

export default route(function (/* { store, ssrContext } */) {
  const createHistory = process.env.SERVER
    ? createMemoryHistory
    : process.env.VUE_ROUTER_MODE === 'history'
      ? createWebHistory
      : createWebHashHistory;

  const Router = createRouter({
    scrollBehavior: (to, from, savedPosition) => {
      if (savedPosition) {
        return savedPosition;
      }
      if (to.path === from.path) {
        // Do not scroll if only query/hash/state changed (e.g. closing a dialog)
        return false;
      }
      return {left: 0, top: 0};
    },
    routes,

    // Leave this as is and make changes in quasar.conf.js instead!
    // quasar.conf.js -> build -> vueRouterMode
    // quasar.conf.js -> build -> publicPath
    history: createHistory(process.env.MODE === 'ssr' ? void 0 : process.env.VUE_ROUTER_BASE),
  });

  const startPageKey = 'tic_ui_start_page';
  const defaultStartPage = '/dashboard';
  const getStartPage = () => localStorage.getItem(startPageKey) || defaultStartPage;

  const canAccessRoute = (path, roles) => {
    const resolved = Router.resolve(path);
    for (const record of resolved.matched) {
      if (record.meta?.requiresAdmin && !roles.includes('admin')) {
        return false;
      }
      if (record.meta?.requiresStreamer && !(roles.includes('admin') || roles.includes('streamer'))) {
        return false;
      }
    }
    return resolved.matched.length > 0;
  };

  const getFallbackStartPage = (roles) => {
    if (roles.includes('admin')) {
      return defaultStartPage;
    }
    if (roles.includes('streamer')) {
      return '/dashboard';
    }
    return '/login';
  };

  // Add navigation guard
  Router.beforeEach(async (to, from, next) => {
    const authStore = useAuthStore();
    if (to.meta.requiresAuth) {
      const canUseWarmSession =
        authStore.isAuthenticated && authStore.token && authStore.user && !authStore.isTokenNearExpiry();
      if (canUseWarmSession) {
        authStore.checkAuthentication().catch((error) => {
          console.error('Background auth check failed:', error);
        });
      } else {
        await authStore.checkAuthentication();
      }
      if (authStore.isAuthenticated) {
        if (to.path === '/') {
          const roles = authStore.user?.roles || [];
          const startPage = getStartPage();
          const target = canAccessRoute(startPage, roles) ? startPage : getFallbackStartPage(roles);
          if (target && target !== to.path) {
            next({path: target, replace: true});
            return;
          }
        }
        if (to.meta.requiresAdmin) {
          const roles = authStore.user?.roles || [];
          if (!roles.includes('admin')) {
            next({path: '/', replace: true});
            return;
          }
        }
        if (to.meta.requiresStreamer) {
          const roles = authStore.user?.roles || [];
          if (!roles.includes('admin') && !roles.includes('streamer')) {
            next({path: '/', replace: true});
            return;
          }
        }
        next();
      } else {
        next({path: '/login', replace: true});
      }
    } else {
      next();
    }
  });

  return Router;
});
