/**
 * DeliveryFlow Documentation Portal JavaScript
 * Theme switching, copy-to-clipboard, responsive navigation, and active TOC scroll-spy.
 */

(function () {
  'use strict';

  // 1. Theme Management (Light / Dark)
  const root = document.documentElement;
  const storedTheme = window.localStorage.getItem("deliveryflow-docs-theme");
  const storageKey = 'deliveryflow-docs-theme';
  const savedTheme = localStorage.getItem(storageKey);

  if (storedTheme === "dark" || storedTheme === "light") {
    root.dataset.theme = storedTheme;
  if (savedTheme === 'dark' || savedTheme === 'light') {
    root.dataset.theme = savedTheme;
  } else if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
    root.dataset.theme = 'dark';
  }

  const themeToggle = document.querySelector("[data-theme-toggle]");
  if (themeToggle) {
    themeToggle.addEventListener("click", () => {
      const nextTheme = root.dataset.theme === "dark" ? "light" : "dark";
      root.dataset.theme = nextTheme;
      window.localStorage.setItem("deliveryflow-docs-theme", nextTheme);
  const themeToggles = document.querySelectorAll('[data-theme-toggle]');
  themeToggles.forEach(toggle => {
    toggle.addEventListener('click', () => {
      const current = root.dataset.theme === 'dark' ? 'dark' : 'light';
      const next = current === 'dark' ? 'light' : 'dark';
      root.dataset.theme = next;
      localStorage.setItem(storageKey, next);
    });
  }
  });

  document.querySelectorAll("[data-copy]").forEach((button) => {
    button.addEventListener("click", async () => {
      const value = button.getAttribute("data-copy");
      if (!value) {
        return;
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

  // 3. Copy to Clipboard for Terminal Commands
  document.querySelectorAll('[data-copy]').forEach((button) => {
    button.addEventListener('click', async () => {
      const textToCopy = button.getAttribute('data-copy');
      if (!textToCopy) return;

      try {
        await navigator.clipboard.writeText(value);
        const original = button.textContent;
        button.textContent = "Copied";
        window.setTimeout(() => {
          button.textContent = original;
        }, 1100);
      } catch (_error) {
        button.setAttribute("title", value);
        await navigator.clipboard.writeText(textToCopy);
        const originalText = button.textContent;
        button.textContent = 'Copied!';
        button.classList.add('copied');
        setTimeout(() => {
          button.textContent = originalText;
          button.classList.remove('copied');
        }, 1500);
      } catch (err) {
        button.setAttribute('title', 'Copy failed, please select manually');
      }
    });
  });

  // 4. Scroll-Spy Table of Contents Highlighting
  const tocLinks = document.querySelectorAll('.toc-nav a');
  if (tocLinks.length > 0) {
    const headings = [];
    tocLinks.forEach(link => {
      const href = link.getAttribute('href');
      if (href && href.startsWith('#')) {
        const target = document.getElementById(href.substring(1));
        if (target) {
          headings.push({ el: target, link: link });
        }
      }
    });

    if (headings.length > 0 && 'IntersectionObserver' in window) {
      const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
          if (entry.isIntersecting) {
            tocLinks.forEach(l => l.classList.remove('active'));
            const match = headings.find(h => h.el === entry.target);
            if (match) {
              match.link.classList.add('active');
            }
          }
        });
      }, {
        rootMargin: '0px 0px -70% 0px',
        threshold: 0.1
      });

      headings.forEach(h => observer.observe(h.el));
    }
  }

  // 5. Client-Side Quick Table Filter (if present)
  document.querySelectorAll('[data-table-filter]').forEach(input => {
    const targetTableId = input.getAttribute('data-table-filter');
    const table = document.getElementById(targetTableId);
    if (!table) return;

    input.addEventListener('input', () => {
      const filterValue = input.value.toLowerCase().trim();
      const rows = table.querySelectorAll('tbody tr');
      rows.forEach(row => {
        const text = row.textContent.toLowerCase();
        row.style.display = text.includes(filterValue) ? '' : 'none';
      });
    });
  });

})();
