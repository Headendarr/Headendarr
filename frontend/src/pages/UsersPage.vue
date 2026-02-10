<template>
  <q-page>

    <div class="q-pa-md">

      <div class="row">
        <div class="col-12 help-main help-main--full">

          <q-card flat>
            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <div class="row items-center q-gutter-md">
                <div class="text-h5">Users</div>
                <q-space />
                <q-btn color="primary" icon="person_add" label="Add User" @click="openCreateDialog" />
              </div>
            </q-card-section>

            <q-separator />

            <q-card-section>
              <q-table
                :rows="users"
                :columns="columns"
                row-key="id"
                flat
                :loading="loading"
              >
                <template v-slot:body-cell-roles="props">
                  <q-td :props="props">
                    {{ props.row.roles.join(', ') }}
                  </q-td>
                </template>

                <template v-slot:body-cell-streaming_key="props">
                  <q-td :props="props">
                    <div class="row items-center no-wrap">
                      <div class="text-mono ellipsis">{{ props.row.streaming_key || '-' }}</div>
                      <q-btn
                        v-if="props.row.streaming_key"
                        dense
                        flat
                        icon="content_copy"
                        class="q-ml-sm"
                        @click="copyStreamingKey(props.row.streaming_key)"
                      />
                    </div>
                  </q-td>
                </template>

                <template v-slot:body-cell-actions="props">
                  <q-td :props="props">
                    <q-btn dense flat icon="edit" @click="openEditDialog(props.row)" />
                    <q-btn dense flat icon="password" @click="openResetPassword(props.row)" />
                  </q-td>
                </template>
              </q-table>
            </q-card-section>
          </q-card>

        </div>
      </div>
    </div>

    <q-dialog v-model="showCreate">
      <q-card style="min-width: 360px;">
        <q-card-section>
          <div class="text-h6">Create User</div>
        </q-card-section>
        <q-card-section>
          <q-input v-model="form.username" label="Username" />
          <q-input v-model="form.password" type="password" label="Password" class="q-mt-md" />
          <q-option-group
            v-model="form.roles"
            :options="roleOptions"
            type="checkbox"
            inline
            class="q-mt-md"
          />
        </q-card-section>
        <q-card-actions align="right">
          <q-btn flat label="Cancel" v-close-popup />
          <q-btn color="primary" label="Create" @click="createUser" />
        </q-card-actions>
      </q-card>
    </q-dialog>

    <q-dialog v-model="showEdit">
      <q-card style="min-width: 360px;">
        <q-card-section>
          <div class="text-h6">Edit User</div>
        </q-card-section>
        <q-card-section>
          <div class="text-subtitle2">{{ editUser?.username }}</div>
          <q-option-group
            v-model="form.roles"
            :options="roleOptions"
            type="checkbox"
            inline
            class="q-mt-md"
          />
          <q-input
            v-model="form.streaming_key"
            readonly
            label="Streaming Key"
            class="q-mt-md"
          >
            <template v-slot:append>
              <q-btn
                dense
                flat
                icon="content_copy"
                @click="copyStreamingKey(form.streaming_key)"
              />
              <q-btn
                dense
                flat
                icon="refresh"
                @click="confirmRotateStreamingKey(editUser)"
              />
            </template>
          </q-input>
          <q-toggle v-model="form.is_active" label="Active" class="q-mt-md" />
        </q-card-section>
        <q-card-actions align="right">
          <q-btn flat label="Cancel" v-close-popup />
          <q-btn color="primary" label="Save" @click="updateUser" />
        </q-card-actions>
      </q-card>
    </q-dialog>
  </q-page>
</template>

<script>
import axios from 'axios';
import {copyToClipboard} from 'quasar';

export default {
  name: 'UsersPage',
  data() {
    return {
      loading: false,
      users: [],
      columns: [
        {name: 'username', label: 'Username', field: 'username', align: 'left'},
        {name: 'roles', label: 'Roles', field: 'roles', align: 'left'},
        {name: 'is_active', label: 'Active', field: 'is_active', align: 'left'},
        {name: 'streaming_key', label: 'Streaming Key', field: 'streaming_key', align: 'left'},
        {name: 'actions', label: '', field: 'actions', align: 'right'},
      ],
      showCreate: false,
      showEdit: false,
      editUser: null,
      form: {
        username: '',
        password: '',
        roles: [],
        is_active: true,
      },
      roleOptions: [
        {label: 'Admin', value: 'admin'},
        {label: 'Streamer', value: 'streamer'},
      ],
    };
  },
  methods: {
    async loadUsers() {
      this.loading = true;
      try {
        const response = await axios.get('/tic-api/users');
        this.users = response.data.data || [];
      } finally {
        this.loading = false;
      }
    },
    openCreateDialog() {
      this.form = {username: '', password: '', roles: ['streamer'], is_active: true};
      this.showCreate = true;
    },
    async createUser() {
      try {
        const response = await axios.post('/tic-api/users', {
          username: this.form.username,
          password: this.form.password,
          roles: this.form.roles,
        });
        this.showCreate = false;
        await this.loadUsers();
      } catch (error) {
        this.$q.notify({color: 'negative', message: 'Failed to create user'});
      }
    },
    openEditDialog(user) {
      this.editUser = user;
      this.form = {
        roles: [...user.roles],
        is_active: user.is_active,
        streaming_key: user.streaming_key || '',
      };
      this.showEdit = true;
    },
    async updateUser() {
      try {
        await axios.put(`/tic-api/users/${this.editUser.id}`, {
          roles: this.form.roles,
          is_active: this.form.is_active,
        });
        this.showEdit = false;
        await this.loadUsers();
      } catch (error) {
        this.$q.notify({color: 'negative', message: 'Failed to update user'});
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
        await axios.post(`/tic-api/users/${user.id}/reset-password`, {password: newPassword});
        this.$q.notify({color: 'green', message: 'Password updated'});
      });
    },
    async confirmRotateStreamingKey(user) {
      if (!user) {
        return;
      }
      this.$q.dialog({
        title: 'Rotate Streaming Key',
        message: 'Are you sure? Rotating this key will invalidate existing playlist/EPG/HDHomeRun URLs for this user.',
        cancel: true,
        ok: {label: 'Rotate'},
        persistent: true,
      }).onOk(async () => {
        await axios.post(`/tic-api/users/${user.id}/rotate-stream-key`);
        await this.loadUsers();
        if (this.editUser && this.editUser.id === user.id) {
          const refreshed = this.users.find((u) => u.id === user.id);
          if (refreshed) {
            this.form.streaming_key = refreshed.streaming_key || '';
          }
        }
      });
    },
    async copyStreamingKey(streamingKey) {
      await copyToClipboard(streamingKey);
      this.$q.notify({color: 'green', message: 'Streaming key copied to clipboard'});
    },
  },
  mounted() {
    this.loadUsers();
  },
};
</script>
