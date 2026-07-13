// users ドメインが所有する driven ポート定義。
// Driven ports owned by the users domain.
import type { User } from './domain/user.js';

export interface UsersRepoPort {
  authenticate(token: string): Promise<User | null>;
  initSchema(): Promise<void>;
  seed(): Promise<void>;
}
