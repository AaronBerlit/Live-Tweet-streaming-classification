/** Design tokens from PRD §9.2 -- a dark-first analytics console. */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "#0B0E14",
        surface: "#141822",
        raised: "#1C2130",
        border: "#262C3B",
        text: { DEFAULT: "#E6E9EF", muted: "#8B93A7", faint: "#80889B" },
        positive: "#34D399",
        negative: "#F87171",
        neutral: "#94A3B8",
        // Interactive only -- never used for a sentiment series.
        accent: "#60A5FA",
        warning: "#FBBF24",
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "sans-serif"],
        mono: ["JetBrains Mono", "ui-monospace", "monospace"],
      },
      borderRadius: { card: "10px" },
      transitionDuration: { DEFAULT: "150ms" },
      transitionTimingFunction: { DEFAULT: "cubic-bezier(0, 0, 0.2, 1)" },
    },
  },
  plugins: [],
};
