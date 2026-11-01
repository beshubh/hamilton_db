# HamiltonDB brand kit

Colors: background `#0B0D10` · white `#F2F4F7` · lime `#C6FF3D` · grey `#8A93A0`
Font: JetBrains Mono (ExtraBold Italic for the name). Text in the SVGs is already converted to shapes, so no font install is needed.

## 1. GitHub README
Copy this folder into your repo as `assets/brand/`, then put this at the top of `README.md`.
It shows the right logo for GitHub light and dark mode:

```html
<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/brand/hamiltondb-logo-dark.svg">
    <img src="assets/brand/hamiltondb-logo-light.svg" alt="HamiltonDB" width="420">
  </picture>
</p>
```

Or use the full-width banner: `![HamiltonDB](assets/brand/png/hamiltondb-banner.png)`

## 2. GitHub social preview (the image shown when the repo link is shared)
Repo → Settings → General → Social preview → upload `png/social-preview.png` (1280×640).

## 3. Avatars (GitHub org, X, Discord)
Upload `png/hamilton-icon-512.png`.

## 4. Docs site / website favicon
Copy `favicon.ico`, `png/apple-touch-icon.png` and `hamilton-icon.svg` to the site root, then in `<head>`:

```html
<link rel="icon" href="/favicon.ico" sizes="any">
<link rel="icon" href="/hamilton-icon.svg" type="image/svg+xml">
<link rel="apple-touch-icon" href="/apple-touch-icon.png">
```

## Files
| File | Use |
|---|---|
| `hamiltondb-logo-dark.svg` | Logo on dark backgrounds |
| `hamiltondb-logo-light.svg` | Logo on light backgrounds |
| `hamilton-icon.svg` | App icon / avatar (dark tile) |
| `hamilton-icon-small.svg` | Simplified icon for 16–48px |
| `hamilton-mark.svg` | Symbol only, transparent, for dark backgrounds |
| `hamiltondb-banner.svg/.png` | README header banner |
| `social-preview.png` | GitHub/X link preview |
| `favicon.ico` | Browser tab icon |

## Rules of thumb
- Keep clear space around the logo of at least the width of one "H" bar.
- Don't stretch, recolor the white parts, or add shadows/glows.
- Below ~48px, use `hamilton-icon-small.svg` (it has no speed streaks, which blur at tiny sizes).

JetBrains Mono is licensed under the SIL Open Font License (see `FONT-LICENSE-OFL.txt`).
