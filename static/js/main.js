/* ═══════════════════════════════════════════════════════════════
   CHAIN TRACKER — Client-side interactions
   Sparklines · Card animations · Tooltips · Hover effects
   ═══════════════════════════════════════════════════════════════ */

(function() {
  'use strict';

  // ── Sparkline Renderer ──
  // Finds all elements with data-sparkline attribute and renders SVG sparklines

  function renderSparklines() {
    var containers = document.querySelectorAll('.sparkline-container[data-sparkline]');

    containers.forEach(function(el) {
      var raw = el.getAttribute('data-sparkline');
      var data;
      try {
        data = JSON.parse(raw);
      } catch(e) {
        return;
      }

      if (!data || !data.length || data.length < 2) return;

      var color = el.getAttribute('data-color') || '#C4A882';
      var w = parseInt(el.getAttribute('data-width')) || 200;
      var h = parseInt(el.getAttribute('data-height')) || 48;

      // Calculate points
      var mn = Math.min.apply(null, data);
      var mx = Math.max.apply(null, data);
      var range = mx - mn || 1;
      var padY = 4;

      var points = data.map(function(v, i) {
        var x = (i / (data.length - 1)) * w;
        var y = h - ((v - mn) / range) * (h - padY * 2) - padY;
        return [x, y];
      });

      // Build path string
      var pathD = points.map(function(p, i) {
        return (i === 0 ? 'M' : 'L') + p[0].toFixed(1) + ',' + p[1].toFixed(1);
      }).join(' ');

      // Area fill path
      var areaD = pathD + ' L' + w + ',' + h + ' L0,' + h + ' Z';

      // Gradient ID
      var gradId = 'sp-' + color.replace('#', '') + '-' + w + '-' + Math.random().toString(36).substr(2, 4);

      // Last point for dot
      var last = points[points.length - 1];

      // Build SVG
      var svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
      svg.setAttribute('width', w);
      svg.setAttribute('height', h);
      svg.style.display = 'block';
      svg.style.overflow = 'visible';

      // Gradient definition
      var defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
      var grad = document.createElementNS('http://www.w3.org/2000/svg', 'linearGradient');
      grad.setAttribute('id', gradId);
      grad.setAttribute('x1', '0');
      grad.setAttribute('y1', '0');
      grad.setAttribute('x2', '0');
      grad.setAttribute('y2', '1');

      var stop1 = document.createElementNS('http://www.w3.org/2000/svg', 'stop');
      stop1.setAttribute('offset', '0%');
      stop1.setAttribute('stop-color', color);
      stop1.setAttribute('stop-opacity', '0.15');

      var stop2 = document.createElementNS('http://www.w3.org/2000/svg', 'stop');
      stop2.setAttribute('offset', '100%');
      stop2.setAttribute('stop-color', color);
      stop2.setAttribute('stop-opacity', '0');

      grad.appendChild(stop1);
      grad.appendChild(stop2);
      defs.appendChild(grad);
      svg.appendChild(defs);

      // Area fill
      var area = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      area.setAttribute('d', areaD);
      area.setAttribute('fill', 'url(#' + gradId + ')');
      svg.appendChild(area);

      // Line path with draw animation
      var line = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      line.setAttribute('d', pathD);
      line.setAttribute('fill', 'none');
      line.setAttribute('stroke', color);
      line.setAttribute('stroke-width', '1.8');
      line.setAttribute('stroke-linecap', 'round');
      line.setAttribute('stroke-linejoin', 'round');
      line.classList.add('sparkline-line');
      svg.appendChild(line);

      // Endpoint dot
      var dot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      dot.setAttribute('cx', last[0].toFixed(1));
      dot.setAttribute('cy', last[1].toFixed(1));
      dot.setAttribute('r', '3');
      dot.setAttribute('fill', '#FFFDF9');
      dot.setAttribute('stroke', color);
      dot.setAttribute('stroke-width', '2');
      dot.classList.add('sparkline-dot');
      svg.appendChild(dot);

      // Clear and append
      el.innerHTML = '';
      el.appendChild(svg);
    });
  }


  // ── Initialize on DOM ready ──
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  function init() {
    renderSparklines();
  }

})();
