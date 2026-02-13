<template>
  <q-page>
    <div class="q-pa-md">
      <div class="row">
        <div class="col-12 help-main help-main--full">
          <q-card flat>
            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <div class="row items-center q-col-gutter-sm justify-between">
                <div :class="$q.screen.lt.sm ? 'col-12' : 'col-auto'">
                  <TicButton
                    label="Add User"
                    icon="person_add"
                    color="primary"
                    :class="$q.screen.lt.sm ? 'full-width' : ''"
                    @click="openCreateDialog"
                  />
                </div>
                <div :class="$q.screen.lt.sm ? 'col-12' : 'col-12 col-sm-6 col-md-4'">
                  <TicSearchInput
                    v-model="searchQuery"
                    label="Search users"
                    placeholder="Username, role, streaming key..."
                  />
                </div>
                <div :class="$q.screen.lt.sm ? 'col-12' : 'col-12 col-sm-6 col-md-3'">
                  <TicSelectInput
                    v-model="roleFilter"
                    label="Role"
                    :options="roleFilterOptions"
                    option-label="label"
                    option-value="value"
                    :emit-value="true"
                    :map-options="true"
                    :clearable="false"
                    :dense="true"
                    :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
                  />
                </div>
              </div>
            </q-card-section>

            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <q-list bordered separator class="rounded-borders">
                <q-item v-for="user in visibleUsers" :key="user.id" class="users-list-item">
                  <q-item-section avatar top>
                    <q-icon name="person" color="primary" />
                  </q-item-section>

                  <q-item-section top>
                    <q-item-label class="row items-center q-gutter-sm">
                      <span class="text-weight-medium">{{ user.username }}</span>
                      <q-chip dense :color="user.is_active ? 'positive' : 'negative'" text-color="white">
                        {{ user.is_active ? 'Active' : 'Disabled' }}
                      </q-chip>
                    </q-item-label>
                    <q-item-label caption> Roles: {{ (user.roles || []).join(', ') || 'None' }}</q-item-label>
                    <q-item-label caption class="row items-center q-gutter-xs no-wrap">
                      <span class="text-weight-medium">Streaming key:</span>
                      <span class="text-mono ellipsis">{{ user.streaming_key || '-' }}</span>
                      <TicActionButton
                        v-if="user.streaming_key"
                        icon="content_copy"
                        color="grey-8"
                        tooltip="Copy streaming key"
                        @click="copyStreamingKey(user.streaming_key)"
                      />
                    </q-item-label>
                  </q-item-section>

                  <q-item-section side top>
                    <TicListActions :actions="userActions(user)" @action="(action) => handleUserAction(action, user)" />
                  </q-item-section>
                </q-item>

                <q-item v-if="!loading && !visibleUsers.length">
                  <q-item-section>
                    <q-item-label class="text-grey-7"> No users found.</q-item-label>
                  </q-item-section>
                </q-item>
              </q-list>

              <div id="users-scroll-anchor" class="users-infinite-anchor">
                <q-infinite-scroll
                  v-if="!loading && hasMoreUsers"
                  ref="usersInfiniteRef"
                  :offset="80"
                  scroll-target="body"
                  @load="onUsersLoad"
                >
                  <template #loading>
                    <div class="row flex-center q-my-md">
                      <q-spinner-dots size="28px" color="primary" />
                    </div>
                  </template>
                </q-infinite-scroll>
              </div>

              <q-inner-loading :showing="loading">
                <q-spinner-dots size="40px" color="primary" />
              </q-inner-loading>
            </q-card-section>
          </q-card>
        </div>
      </div>
    </div>

    <TicDialogWindow
      v-model="showCreate"
      title="Create User"
      width="520px"
      :actions="createDialogActions"
      @action="onCreateDialogAction"
    >
      <div class="q-pa-md">
        <q-form class="tic-form-layout">
          <TicTextInput v-model="form.username" label="Username" description="Unique username for this account." />

          <TicTextInput
            v-model="form.password"
            type="password"
            label="Password"
            description="Set an initial password for this user."
          />

          <TicSelectInput
            v-model="form.roles"
            label="Roles"
            description="Assign one or more access roles."
            :options="roleOptions"
            option-label="label"
            option-value="value"
            :emit-value="true"
            :map-options="true"
            :multiple="true"
            :clearable="false"
            :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
          />
        </q-form>
      </div>
    </TicDialogWindow>

    <TicDialogWindow
      v-model="showEdit"
      :title="`Edit User: ${editUser?.username || ''}`"
      width="560px"
      :actions="editDialogActions"
      @action="onEditDialogAction"
    >
      <div class="q-pa-md">
        <q-form class="tic-form-layout">
          <TicSelectInput
            v-model="form.roles"
            label="Roles"
            description="Assign one or more access roles."
            :options="roleOptions"
            option-label="label"
            option-value="value"
            :emit-value="true"
            :map-options="true"
            :multiple="true"
            :clearable="false"
            :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
          />

          <TicToggleInput
            v-model="form.is_active"
            label="Active"
            description="Disable this account to block logins and API use."
          />

          <TicTextInput
            v-model="form.streaming_key"
            readonly
            label="Streaming Key"
            description="Used for playlist, EPG, and HDHomeRun endpoints."
          >
            <template #append>
              <TicActionButton
                icon="content_copy"
                color="grey-8"
                tooltip="Copy streaming key"
                @click="copyStreamingKey(form.streaming_key)"
              />
              <TicActionButton
                icon="refresh"
                color="warning"
                tooltip="Rotate streaming key"
                @click="confirmRotateStreamingKey(editUser)"
              />
            </template>
          </TicTextInput>
        </q-form>
      </div>
    </TicDialogWindow>
  </q-page>
</template>

<script>
import axios from 'axios';
import {copyToClipboard} from 'quasar';
import {
  TicActionButton,
  TicButton,
  TicDialogWindow,
  TicListActions,
  TicSearchInput,
  TicSelectInput,
  TicTextInput,
  TicToggleInput,
} from 'components/ui';

const USERS_PAGE_SIZE = 40;

export default {
  name: 'UsersPage',
  components: {
    TicActionButton,
    TicButton,
    TicDialogWindow,
    TicListActions,
    TicSearchInput,
    TicSelectInput,
    TicTextInput,
    TicToggleInput,
  },
  data() {
    return {
      loading: false,
      creating: false,
      saving: false,
      users: [],
      searchQuery: '',
      roleFilter: null,
      showCreate: false,
      showEdit: false,
      editUser: null,
      visibleUsersCount: USERS_PAGE_SIZE,
      form: {
        username: '',
        password: '',
        roles: [],
        is_active: true,
        streaming_key: '',
      },
      roleOptions: [
        {label: 'Admin', value: 'admin'},
        {label: 'Streamer', value: 'streamer'},
      ],
    };
  },
  computed: {
    roleFilterOptions() {
      return [{label: 'All', value: null}, ...this.roleOptions];
    },
    filteredUsers() {
      const query = String(this.searchQuery || '').trim().toLowerCase();
      const role = this.roleFilter;
      return (this.users || []).filter((user) => {
        const roles = Array.isArray(user.roles) ? user.roles : [];
        if (role && !roles.includes(role)) {
          return false;
        }
        if (!query) {
          return true;
        }
        const values = [user.username, user.streaming_key, roles.join(' '), user.is_active ? 'active' : 'disabled'];
        return values.some((value) =>
          String(value || '').toLowerCase().includes(query),
        );
      }).sort((a, b) => String(a.username || '').localeCompare(String(b.username || '')));
    },
    visibleUsers() {
      return this.filteredUsers.slice(0, this.visibleUsersCount);
    },
    hasMoreUsers() {
      return this.visibleUsersCount < this.filteredUsers.length;
    },
    createDialogActions() {
      return [
        {
          id: 'create',
          label: 'Create',
          icon: 'save',
          color: 'positive',
          loading: this.creating,
        },
      ];
    },
    editDialogActions() {
      return [
        {
          id: 'save',
          label: 'Save',
          icon: 'save',
          color: 'positive',
          loading: this.saving,
        },
      ];
    },
  },
  watch: {
    searchQuery() {
      this.resetVisibleUsers();
    },
    roleFilter() {
      this.resetVisibleUsers();
    },
  },
  methods: {
    async loadUsers() {
      this.loading = true;
      try {
        const response = await axios.get('/tic-api/users');
        this.users = response.data.data || [];
        this.resetVisibleUsers();
      } finally {
        this.loading = false;
      }
    },
    resetVisibleUsers() {
      this.visibleUsersCount = USERS_PAGE_SIZE;
      this.$nextTick(() => {
        if (this.$refs.usersInfiniteRef) {
          this.$refs.usersInfiniteRef.reset();
        }
      });
    },
    onUsersLoad(index, done) {
      if (!this.hasMoreUsers) {
        done(true);
        return;
      }
      this.visibleUsersCount += USERS_PAGE_SIZE;
      done(this.visibleUsersCount >= this.filteredUsers.length);
    },
    userActions(user) {
      return [
        {
          id: 'edit',
          icon: 'edit',
          label: 'Edit user',
          color: 'primary',
          tooltip: `Edit ${user.username}`,
        },
        {
          id: 'reset-password',
          icon: 'password',
          label: 'Reset password',
          color: 'warning',
          tooltip: `Reset password for ${user.username}`,
        },
      ];
    },
    handleUserAction(action, user) {
      if (action.id === 'edit') {
        this.openEditDialog(user);
      }
      if (action.id === 'reset-password') {
        this.openResetPassword(user);
      }
    },
    openCreateDialog() {
      this.form = {
        username: '',
        password: '',
        roles: ['streamer'],
        is_active: true,
        streaming_key: '',
      };
      this.showCreate = true;
    },
    onCreateDialogAction(action) {
      if (action.id === 'create') {
        this.createUser();
      }
    },
    async createUser() {
      if (!this.form.username || !this.form.password) {
        this.$q.notify({color: 'negative', message: 'Username and password are required'});
        return;
      }
      this.creating = true;
      try {
        await axios.post('/tic-api/users', {
          username: this.form.username,
          password: this.form.password,
          roles: this.form.roles,
        });
        this.showCreate = false;
        await this.loadUsers();
      } catch {
        this.$q.notify({color: 'negative', message: 'Failed to create user'});
      } finally {
        this.creating = false;
      }
    },
    openEditDialog(user) {
      this.editUser = user;
      this.form = {
        roles: [...(user.roles || [])],
        is_active: Boolean(user.is_active),
        streaming_key: user.streaming_key || '',
      };
      this.showEdit = true;
    },
    onEditDialogAction(action) {
      if (action.id === 'save') {
        this.updateUser();
      }
    },
    async updateUser() {
      if (!this.editUser) {
        return;
      }
      this.saving = true;
      try {
        await axios.put(`/tic-api/users/${this.editUser.id}`, {
          roles: this.form.roles,
          is_active: this.form.is_active,
        });
        this.showEdit = false;
        await this.loadUsers();
      } catch {
        this.$q.notify({color: 'negative', message: 'Failed to update user'});
      } finally {
        this.saving = false;
      }
    },
    openResetPassword(user) {
      this.$q.dialog({
        title: `Reset Password (${user.username})`,
        message: 'Enter a new password',
        prompt: {
          model: '',
          type: 'password',
        },
        cancel: true,
      }).onOk(async (newPassword) => {
        if (!newPassword) {
          return;
        }
        await axios.post(`/tic-api/users/${user.id}/reset-password`, {password: newPassword});
        this.$q.notify({color: 'positive', message: 'Password updated'});
      });
    },
    async confirmRotateStreamingKey(user) {
      if (!user) {
        return;
      }
      this.$q.dialog({
        title: 'Rotate Streaming Key',
        message: 'Rotate this key? Existing playlist/EPG/HDHomeRun URLs for this user will stop working.',
        cancel: true,
        persistent: true,
        ok: {
          label: 'Rotate',
          color: 'negative',
        },
      }).onOk(async () => {
        await axios.post(`/tic-api/users/${user.id}/rotate-stream-key`);
        await this.loadUsers();
        if (this.editUser && this.editUser.id === user.id) {
          const refreshed = this.users.find((item) => item.id === user.id);
          if (refreshed) {
            this.form.streaming_key = refreshed.streaming_key || '';
          }
        }
      });
    },
    async copyStreamingKey(streamingKey) {
      if (!streamingKey) {
        return;
      }
      await copyToClipboard(streamingKey);
      this.$q.notify({color: 'positive', message: 'Streaming key copied to clipboard'});
    },
  },
  mounted() {
    this.loadUsers();
  },
};
</script>

<style scoped>
.users-list-item {
  align-items: flex-start;
}

.users-infinite-anchor {
  min-height: 1px;
}

@media (max-width: 1023px) {
  .users-list-item :deep(.q-item__section--side) {
    padding-left: 8px;
  }
}
</style>
