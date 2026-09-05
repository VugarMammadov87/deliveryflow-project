/**
 * DeliveryFlow Documentation Portal JavaScript
 * Theme switching, copy-to-clipboard, responsive navigation drawer, and active TOC scroll-spy.
 * Zero external dependencies. 100% offline-compatible.
 */

(function () {
  'use strict';

  // 1. Theme Management (Light / Dark)
  const root = document.documentElement;
  const storageKey = 'deliveryflow-docs-theme';

  function getInitialTheme() {
    try {
      const saved = localStorage.getItem(storageKey);
      if (saved === 'dark' || saved === 'light') {
        return saved;
      }
    } catch (_err) {}

    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
      return 'dark';
    }
    return 'light';
  }

  function applyTheme(theme) {
    root.dataset.theme = theme;
    try {
      localStorage.setItem(storageKey, theme);
    } catch (_err) {}
  }

  applyTheme(getInitialTheme());

  const themeToggles = document.querySelectorAll('[data-theme-toggle]');
  themeToggles.forEach((toggle) => {
    toggle.addEventListener('click', () => {
      const current = root.dataset.theme === 'dark' ? 'dark' : 'light';
      const next = current === 'dark' ? 'light' : 'dark';
      applyTheme(next);
    });
  });

  // 2. Mobile Sidebar Navigation Drawer
  const menuBtn = document.querySelector('[data-mobile-menu]');
  const sidebar = document.querySelector('.sidebar');

  if (menuBtn && sidebar) {
    menuBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      sidebar.classList.toggle('open');
    });

    document.addEventListener('click', (e) => {
      if (!sidebar.contains(e.target) && !menuBtn.contains(e.target)) {
        sidebar.classList.remove('open');
      }
    });
  }

  // 3. Copy to Clipboard for Terminal Commands & Code Blocks
  document.querySelectorAll('[data-copy], .btn-copy').forEach((button) => {
    button.addEventListener('click', async () => {
      let textToCopy = button.getAttribute('data-copy');
      if (!textToCopy) {
        // Fallback: look for nearby code inside a parent container
        const container = button.closest('.code-block, .terminal-card');
        if (container) {
          const codeEl = container.querySelector('pre, .terminal-body');
          if (codeEl) {
            textToCopy = codeEl.innerText.trim();
          }
        }
      }
      if (!textToCopy) return;

      try {
        await navigator.clipboard.writeText(textToCopy);
        const originalText = button.textContent;
        button.textContent = 'Copied!';
        button.classList.add('copied');
        setTimeout(() => {
          button.textContent = originalText;
          button.classList.remove('copied');
        }, 1500);
      } catch (_err) {
        button.setAttribute('title', 'Copy failed, please select manually');
      }
    });
  });

  // 4. Scroll-Spy Table of Contents Highlighting
  const tocLinks = document.querySelectorAll('.toc-nav a, .toc a, .doc-toc a');
  if (tocLinks.length > 0 && 'IntersectionObserver' in window) {
    const headings = [];
    tocLinks.forEach((link) => {
      const href = link.getAttribute('href');
      if (href && href.startsWith('#')) {
        const target = document.getElementById(href.substring(1));
        if (target) {
          headings.push({ el: target, link: link });
        }
      }
    });

    if (headings.length > 0) {
      const observer = new IntersectionObserver(
        (entries) => {
          entries.forEach((entry) => {
            if (entry.isIntersecting) {
              tocLinks.forEach((l) => l.classList.remove('active'));
              const match = headings.find((h) => h.el === entry.target);
              if (match) {
                match.link.classList.add('active');
              }
            }
          });
        },
        {
          rootMargin: '0px 0px -70% 0px',
          threshold: 0.1,
        }
      );

      headings.forEach((h) => observer.observe(h.el));
    }
  }

  // 5. Client-Side Quick Table Filter
  document.querySelectorAll('[data-table-filter]').forEach((input) => {
    const targetTableId = input.getAttribute('data-table-filter');
    const table = document.getElementById(targetTableId);
    if (!table) return;

    input.addEventListener('input', () => {
      const filterValue = input.value.toLowerCase().trim();
      const rows = table.querySelectorAll('tbody tr');
      rows.forEach((row) => {
        const text = row.textContent.toLowerCase();
        row.style.display = text.includes(filterValue) ? '' : 'none';
      });
    });
  });
})();
