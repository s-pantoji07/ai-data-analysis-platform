/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        brand: {
          yellow: '#F2C811',
          dark: '#333333',
          light: '#F3F2F1',
          border: '#EDEBE9'
        }
      }
    },
  },
  plugins: [],
}