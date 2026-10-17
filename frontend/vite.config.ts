import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// In development the API runs on the host (`make api`, or the compose service on
// API_PORT); requests to /api are proxied there, as nginx does in the image.
const api = process.env.FRAUDX_API ?? "http://localhost:8000";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: { "/api": { target: api, changeOrigin: true, rewrite: (p) => p.replace(/^\/api/, "") } },
  },
});
