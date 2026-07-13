// items ドメインの純粋型。I/O 依存なし。
// Pure Item type. Zero I/O imports.

export interface Item {
  id: number;
  name: string;
  stock: number;
  created_at: string;
}
