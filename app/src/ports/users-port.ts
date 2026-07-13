// UsersPort: orders が users ドメインにアクセスするための driven ポート。
// UsersPort: driven port through which orders accesses the users domain.
import type { UsersRepoPort } from '../domains/users/ports.js';
import type { User } from '../domains/users/domain/user.js';

export type { User };

export interface UsersPort {
  authenticate(token: string): Promise<User | null>;
}

// インプロセスアダプタ: 同一プロセス内の users リポジトリを直接呼ぶ。
// In-process adapter: delegates to the users domain repo (monolith/modular-monolith).
export class InProcessUsersAdapter implements UsersPort {
  constructor(private readonly usersRepo: UsersRepoPort) {}

  authenticate(token: string): Promise<User | null> {
    return this.usersRepo.authenticate(token);
  }
}

// HTTP アダプタ: 別プロセスの users サービスに HTTP 経由でアクセス（マイクロサービス）。
// HTTP adapter: reaches users service over HTTP (microservice deployment).
export class HttpUsersAdapter implements UsersPort {
  constructor(private readonly baseUrl: string) {}

  async authenticate(token: string): Promise<User | null> {
    const res = await fetch(`${this.baseUrl}/internal/auth`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token }),
      signal: AbortSignal.timeout(2000),
    });
    // 401 は「認証失敗」— null を返す（例外なし）。
    // 401 means "not authenticated" — return null (no throw).
    if (res.status === 401) return null;
    if (!res.ok) throw new Error(`users service authenticate failed: ${res.status}`);
    const data = await res.json() as { user: User };
    return data.user;
  }
}
