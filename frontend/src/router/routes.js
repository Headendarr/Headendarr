const routes = [
  {
    path: '/',
    component: () => import('layouts/MainLayout.vue'),
    children: [
      {
        path: '',
        component: () => import('pages/DashboardPage.vue'),
        meta: {requiresAuth: true},
      },
    ],
  },
  {
    name: 'dashboard',
    path: '/dashboard',
    component: () => import('layouts/MainLayout.vue'),
    children: [
      {
        path: '',
        component: () => import('pages/DashboardPage.vue'),
        meta: {requiresAuth: true},
      },
    ],
  },
  {
    name: 'audit',
    path: '/audit',
    component: () => import('layouts/MainLayout.vue'),
    children: [
      {
        path: '',
        component: () => import('pages/AuditPage.vue'),
        meta: {requiresAuth: true, requiresAdmin: true},
      },
    ],
  },
  {
    name: 'settings',
    path: '/settings',
    component: () => import('layouts/MainLayout.vue'),
    children: [
      {
        path: '',
        component: () => import('pages/SettingsPage.vue'),
        meta: {requiresAuth: true, requiresAdmin: true},
      },
    ],
  },
  {
    path: '/general',
    redirect: '/settings',
  },
  {
    name: 'tvheadend',
    path: '/tvheadend',
    component: () => import('layouts/MainLayout.vue'),
    children: [
      {
        path: '',
        component: () => import('pages/TvheadendPage.vue'),
        meta: {requiresAuth: true, requiresAdmin: true},
      },
    ],
  },
  {
    name: 'playlists',
    path: '/playlists',
    component: () => import('layouts/MainLayout.vue'),
    children: [
      {
        path: '',
        component: () => import('pages/PlaylistsPage.vue'),
        meta: {requiresAuth: true, requiresAdmin: true},
      },
    ],
  },
  {
    name: 'epgs',
    path: '/epgs',
    component: () => import('layouts/MainLayout.vue'),
    children: [
      {
        path: '',
        component: () => import('pages/EpgsPage.vue'),
        meta: {requiresAuth: true, requiresAdmin: true},
      },
    ],
  },
  {
    name: 'channels',
    path: '/channels',
    component: () => import('layouts/MainLayout.vue'),
    children: [
      {
        path: '',
        component: () => import('pages/ChannelsPage.vue'),
        meta: {requiresAuth: true, requiresAdmin: true},
      },
    ],
  },
  {
    name: 'tv-guide',
    path: '/guide',
    component: () => import('layouts/MainLayout.vue'),
    children: [
      {
        path: '',
        component: () => import('pages/TvGuidePage.vue'),
        meta: {requiresAuth: true, requiresStreamer: true},
      },
    ],
  },
  {
    name: 'dvr',
    path: '/dvr',
    component: () => import('layouts/MainLayout.vue'),
    children: [
      {
        path: '',
        component: () => import('pages/DvrPage.vue'),
        meta: {requiresAuth: true, requiresStreamer: true},
      },
    ],
  },
  {
    name: 'users',
    path: '/users',
    component: () => import('layouts/MainLayout.vue'),
    children: [
      {
        path: '',
        component: () => import('pages/UsersPage.vue'),
        meta: {requiresAuth: true, requiresAdmin: true},
      },
    ],
  },
  {
    name: 'user-settings',
    path: '/user-settings',
    component: () => import('layouts/MainLayout.vue'),
    children: [
      {
        path: '',
        component: () => import('pages/UserSettingsPage.vue'),
        meta: {requiresAuth: true},
      },
    ],
  },
  {
    name: 'login',
    path: '/login',
    component: () => import('pages/LoginPage.vue'),
  },

  // Always leave this as last one,
  // but you can also remove it
  {
    path: '/:catchAll(.*)*',
    component: () => import('pages/ErrorNotFound.vue'),
  },
];

export default routes;
