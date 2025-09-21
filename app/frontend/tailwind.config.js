/** @type {import('tailwindcss').Config} */
export default {
  darkMode: ["class"],
  content: [
    "./index.html",                  // Vite entry
    "./src/**/*.{js,ts,jsx,tsx}",    // all React components
  ],
  theme: {
    extend: {
      // your colors, spacing, borderRadius, etc.
    },
  },
  plugins: [
    require('@tailwindcss/typography'),
    require('@tailwindcss/forms'),
    require('tailwindcss-animate'),
  ],
}
