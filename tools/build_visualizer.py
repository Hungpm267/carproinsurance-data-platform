import os
import json

def collect_pipelines():
    base_dir = "data pipelines"
    pipelines = {}
    
    for root, dirs, files in os.walk(base_dir):
        for f in files:
            if f == "pipeline-content.json":
                folder_name = os.path.basename(root)
                pipeline_name = folder_name.replace(".DataPipeline", "")
                rel_path = os.path.relpath(root, base_dir)
                is_archive = "archive" in rel_path
                file_path = os.path.join(root, f)
                try:
                    with open(file_path, "r", encoding="utf-8") as fp:
                        content = json.load(fp)
                        key = f"{pipeline_name} (archive)" if is_archive else pipeline_name
                        pipelines[key] = {
                            "name": pipeline_name,
                            "is_archive": is_archive,
                            "folder": rel_path,
                            "properties": content.get("properties", {}),
                            "raw": content
                        }
                except Exception as e:
                    print(f"Error loading {file_path}: {e}")
    return pipelines

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>CarPro Fabric Pipeline Visualizer (Excalidraw Style)</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Caveat:wght@600;700&family=Kalam:wght@300;400;700&family=Patrick+Hand&family=Fira+Code:wght@400;500&display=swap" rel="stylesheet">
  <script src="https://cdnjs.cloudflare.com/ajax/libs/rough.js/3.1.0/rough.js"></script>
  <style>
    :root {
      --bg-color: #fbf9f5;
      --grid-dot: #ded8ce;
      --text-main: #242220;
      --text-muted: #6f6b64;
      --accent: #2b6cb0;
      --border-rough: #2b2b2b;
      --font-hand: 'Kalam', 'Caveat', 'Patrick Hand', cursive, sans-serif;
      --font-code: 'Fira Code', monospace;
    }

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
      user-select: none;
    }

    body {
      background-color: var(--bg-color);
      font-family: var(--font-hand);
      color: var(--text-main);
      overflow: hidden;
      width: 100vw;
      height: 100vh;
      display: flex;
      flex-direction: column;
    }

    /* Sketchy Background Pattern */
    .canvas-container {
      position: relative;
      flex: 1;
      width: 100%;
      height: 100%;
      overflow: hidden;
      cursor: grab;
      background-image: radial-gradient(var(--grid-dot) 1.5px, transparent 1.5px);
      background-size: 24px 24px;
    }

    .canvas-container:active {
      cursor: grabbing;
    }

    #viewport {
      position: absolute;
      top: 0;
      left: 0;
      transform-origin: 0 0;
      will-change: transform;
    }

    #roughSvg {
      position: absolute;
      top: 0;
      left: 0;
      overflow: visible;
      pointer-events: none;
    }

    /* Top Control Bar */
    .top-navbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 10px 20px;
      background: rgba(251, 249, 245, 0.95);
      border-bottom: 2px solid #2b2b2b;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.05);
      z-index: 100;
      gap: 15px;
    }

    .brand-title {
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 24px;
      font-weight: 700;
      color: #1a202c;
      letter-spacing: -0.5px;
    }

    .brand-badge {
      font-size: 13px;
      background: #fed7aa;
      border: 1.5px solid #2b2b2b;
      padding: 2px 8px;
      border-radius: 12px;
      font-family: var(--font-hand);
      transform: rotate(-2deg);
    }

    .controls-group {
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .custom-select {
      font-family: var(--font-hand);
      font-size: 17px;
      font-weight: 700;
      padding: 6px 14px;
      background: #ffffff;
      border: 2px solid #2b2b2b;
      border-radius: 8px;
      cursor: pointer;
      box-shadow: 2px 2px 0px #2b2b2b;
      outline: none;
      transition: all 0.15s;
    }

    .custom-select:hover {
      box-shadow: 3px 3px 0px #2b2b2b;
      transform: translate(-1px, -1px);
    }

    .sketch-btn {
      font-family: var(--font-hand);
      font-size: 16px;
      font-weight: 700;
      padding: 6px 14px;
      background: #ffffff;
      border: 2px solid #2b2b2b;
      border-radius: 8px;
      cursor: pointer;
      box-shadow: 2px 2px 0px #2b2b2b;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      transition: all 0.15s;
    }

    .sketch-btn:hover {
      box-shadow: 3px 3px 0px #2b2b2b;
      transform: translate(-1px, -1px);
      background: #fdf6b2;
    }

    .sketch-btn:active {
      box-shadow: 1px 1px 0px #2b2b2b;
      transform: translate(1px, 1px);
    }

    .zoom-controls {
      display: flex;
      align-items: center;
      gap: 4px;
      background: #ffffff;
      border: 2px solid #2b2b2b;
      border-radius: 8px;
      padding: 2px 4px;
      box-shadow: 2px 2px 0px #2b2b2b;
    }

    .zoom-btn {
      background: transparent;
      border: none;
      font-family: var(--font-hand);
      font-size: 18px;
      font-weight: 700;
      width: 28px;
      height: 28px;
      cursor: pointer;
      border-radius: 4px;
    }

    .zoom-btn:hover {
      background: #e2e8f0;
    }

    .zoom-level {
      font-size: 15px;
      font-weight: 700;
      min-width: 48px;
      text-align: center;
    }

    /* Node on Canvas */
    .sketch-node {
      position: absolute;
      width: 250px;
      min-height: 80px;
      cursor: move;
      pointer-events: auto;
      z-index: 10;
      transition: box-shadow 0.2s;
    }

    .node-content {
      position: relative;
      padding: 12px 14px;
      z-index: 2;
    }

    .node-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 6px;
    }

    .node-type-badge {
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      padding: 1px 8px;
      border-radius: 6px;
      border: 1.5px solid #2b2b2b;
      background: #ffffff;
      letter-spacing: 0.5px;
      transform: rotate(-1deg);
    }

    .node-name {
      font-size: 17px;
      font-weight: 700;
      color: #1a202c;
      word-break: break-word;
      line-height: 1.2;
    }

    .node-desc {
      font-size: 13px;
      color: #4a5568;
      margin-top: 4px;
      line-height: 1.2;
      font-family: var(--font-hand);
    }

    .node-sub-count {
      font-size: 12px;
      font-weight: 700;
      color: #9b2c2c;
      margin-top: 4px;
      display: inline-block;
      background: #fee2e2;
      padding: 2px 6px;
      border-radius: 4px;
      border: 1px dashed #2b2b2b;
    }

    /* Selected state */
    .sketch-node.selected .node-content {
      outline: 2px dashed #3182ce;
      outline-offset: 4px;
      border-radius: 8px;
    }

    /* Details Drawer */
    .details-drawer {
      position: fixed;
      right: 0;
      top: 61px;
      bottom: 0;
      width: 440px;
      background: #ffffff;
      border-left: 2px solid #2b2b2b;
      box-shadow: -6px 0 20px rgba(0, 0, 0, 0.08);
      z-index: 90;
      display: flex;
      flex-direction: column;
      transform: translateX(100%);
      transition: transform 0.25s cubic-bezier(0.16, 1, 0.3, 1);
    }

    .details-drawer.open {
      transform: translateX(0);
    }

    .drawer-header {
      padding: 16px 20px;
      border-bottom: 2px solid #2b2b2b;
      background: #fef08a;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }

    .drawer-title {
      font-size: 20px;
      font-weight: 700;
    }

    .drawer-close {
      background: transparent;
      border: 2px solid #2b2b2b;
      border-radius: 6px;
      font-family: var(--font-hand);
      font-size: 18px;
      font-weight: 700;
      width: 32px;
      height: 32px;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
    }

    .drawer-close:hover {
      background: #fca5a5;
    }

    .drawer-body {
      flex: 1;
      padding: 20px;
      overflow-y: auto;
      user-select: text;
    }

    .info-section {
      margin-bottom: 18px;
    }

    .info-label {
      font-size: 14px;
      font-weight: 700;
      color: #718096;
      text-transform: uppercase;
      margin-bottom: 4px;
    }

    .info-value {
      font-size: 16px;
      color: #1a202c;
      word-break: break-word;
    }

    .code-box {
      font-family: var(--font-code);
      font-size: 13px;
      background: #1e1e1e;
      color: #d4d4d4;
      padding: 12px;
      border-radius: 6px;
      border: 1.5px solid #2b2b2b;
      overflow-x: auto;
      margin-top: 6px;
      white-space: pre-wrap;
      user-select: text;
    }

    .badge-tag {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 6px;
      border: 1.5px solid #2b2b2b;
      font-size: 13px;
      font-weight: 700;
      margin-right: 6px;
      margin-bottom: 6px;
    }

    /* Bottom Status/Legend bar */
    .bottom-legend {
      position: absolute;
      bottom: 16px;
      left: 20px;
      background: rgba(255, 255, 255, 0.92);
      border: 2px solid #2b2b2b;
      border-radius: 10px;
      padding: 8px 16px;
      display: flex;
      align-items: center;
      gap: 16px;
      box-shadow: 3px 3px 0px #2b2b2b;
      z-index: 50;
      font-size: 14px;
      font-weight: 700;
    }

    .legend-item {
      display: flex;
      align-items: center;
      gap: 6px;
    }

    .legend-color {
      width: 14px;
      height: 14px;
      border: 1.5px solid #2b2b2b;
      border-radius: 3px;
    }

    .subactivity-modal {
      position: fixed;
      top: 0;
      left: 0;
      right: 0;
      bottom: 0;
      background: rgba(0, 0, 0, 0.5);
      z-index: 150;
      display: none;
      align-items: center;
      justify-content: center;
      backdrop-filter: blur(2px);
    }

    .subactivity-modal.open {
      display: flex;
    }

    .modal-box {
      background: #ffffff;
      border: 3px solid #2b2b2b;
      border-radius: 12px;
      box-shadow: 8px 8px 0px #2b2b2b;
      width: 80%;
      max-width: 900px;
      max-height: 85vh;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }

    .modal-header {
      padding: 16px 20px;
      background: #c7d2fe;
      border-bottom: 2px solid #2b2b2b;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }

    .modal-body {
      padding: 20px;
      overflow-y: auto;
      flex: 1;
    }

    /* Activity colors palette (Pastels with hand-drawn warmth) */
    .color-notebook { background-color: #fef08a; }       /* Light Yellow */
    .color-lookup { background-color: #bae6fd; }         /* Soft Sky */
    .color-copy { background-color: #bbf7d0; }           /* Mint Green */
    .color-foreach { background-color: #e9d5ff; }        /* Lavender */
    .color-ifcondition { background-color: #fed7aa; }    /* Peach/Orange */
    .color-filter { background-color: #fbcfe8; }         /* Soft Pink */
    .color-storedproc { background-color: #99f6e4; }     /* Teal */
    .color-invokepipeline { background-color: #ddd6fe; } /* Purple */
    .color-script { background-color: #fde047; }         /* Yellow */
    .color-default { background-color: #ffffff; }

  </style>
</head>
<body>

  <!-- Top Navbar -->
  <div class="top-navbar">
    <div class="brand-title">
      <span>CarPro Insurance</span>
      <span class="brand-badge">Microsoft Fabric Pipeline Studio</span>
    </div>

    <div class="controls-group">
      <label for="pipelineSelect" style="font-weight: 700;">Pipeline:</label>
      <select id="pipelineSelect" class="custom-select"></select>

      <button id="btnAutoLayout" class="sketch-btn" title="Auto-organize nodes into layered topological order">
        <span>Auto-Layout</span>
      </button>

      <button id="btnResetView" class="sketch-btn" title="Center diagram in screen">
        <span>Center View</span>
      </button>

      <div class="zoom-controls">
        <button id="btnZoomOut" class="zoom-btn">-</button>
        <span id="zoomLevel" class="zoom-level">100%</span>
        <button id="btnZoomIn" class="zoom-btn">+</button>
      </div>
    </div>
  </div>

  <!-- Main Canvas Container -->
  <div class="canvas-container" id="canvasContainer">
    <div id="viewport">
      <svg id="roughSvg"></svg>
      <div id="nodesContainer"></div>
    </div>
  </div>

  <!-- Bottom Legend -->
  <div class="bottom-legend">
    <span style="color: var(--text-muted);">Activities:</span>
    <div class="legend-item"><div class="legend-color color-notebook"></div><span>Notebook (PySpark)</span></div>
    <div class="legend-item"><div class="legend-color color-lookup"></div><span>Lookup</span></div>
    <div class="legend-item"><div class="legend-color color-copy"></div><span>Copy Data</span></div>
    <div class="legend-item"><div class="legend-color color-foreach"></div><span>ForEach</span></div>
    <div class="legend-item"><div class="legend-color color-ifcondition"></div><span>IfCondition</span></div>
    <div class="legend-item"><div class="legend-color color-storedproc"></div><span>Stored Procedure</span></div>
    <div class="legend-item"><div class="legend-color color-invokepipeline"></div><span>Invoke Pipeline</span></div>
    <span style="margin-left: 10px; color: var(--text-muted);">| Drag nodes to arrange | Click node for properties</span>
  </div>

  <!-- Activity Detail Drawer -->
  <div class="details-drawer" id="detailsDrawer">
    <div class="drawer-header">
      <div class="drawer-title" id="drawerTitle">Activity Details</div>
      <button class="drawer-close" id="drawerClose">&times;</button>
    </div>
    <div class="drawer-body" id="drawerBody"></div>
  </div>

  <!-- Sub-activities Modal (for ForEach / IfCondition child activities) -->
  <div class="subactivity-modal" id="subModal">
    <div class="modal-box">
      <div class="modal-header">
        <h2 id="modalTitle" style="font-family: var(--font-hand); font-size: 22px;">Nested Child Activities</h2>
        <button class="drawer-close" id="modalClose">&times;</button>
      </div>
      <div class="modal-body" id="modalBody"></div>
    </div>
  </div>

  <script>
    // Embedded Pipeline JSON Data
    const PIPELINES_DATA = __PIPELINES_DATA_PLACEHOLDER__;

    // Type Color Mapping
    const TYPE_COLORS = {
      'TridentNotebook': { fill: '#fef08a', class: 'color-notebook', label: 'PySpark Notebook' },
      'Lookup': { fill: '#bae6fd', class: 'color-lookup', label: 'Lookup' },
      'Copy': { fill: '#bbf7d0', class: 'color-copy', label: 'Copy Data' },
      'ForEach': { fill: '#e9d5ff', class: 'color-foreach', label: 'ForEach Loop' },
      'IfCondition': { fill: '#fed7aa', class: 'color-ifcondition', label: 'If Condition' },
      'Filter': { fill: '#fbcfe8', class: 'color-filter', label: 'Filter' },
      'SqlServerStoredProcedure': { fill: '#99f6e4', class: 'color-storedproc', label: 'Stored Proc' },
      'InvokePipeline': { fill: '#ddd6fe', class: 'color-invokepipeline', label: 'Invoke Pipeline' },
      'Script': { fill: '#fde047', class: 'color-script', label: 'SQL Script' },
      'default': { fill: '#ffffff', class: 'color-default', label: 'Activity' }
    };

    function getTypeMeta(type) {
      return TYPE_COLORS[type] || TYPE_COLORS['default'];
    }

    // State
    let currentPipelineKey = Object.keys(PIPELINES_DATA)[0] || '';
    let nodePositions = {};
    let selectedNodeName = null;
    let zoom = 1.0;
    let panX = 60;
    let panY = 60;
    let isPanning = false;
    let startPanX = 0;
    let startPanY = 0;
    let isDraggingNode = false;
    let draggedNodeName = null;
    let dragOffsetX = 0;
    let dragOffsetY = 0;

    const viewport = document.getElementById('viewport');
    const roughSvg = document.getElementById('roughSvg');
    const nodesContainer = document.getElementById('nodesContainer');
    const canvasContainer = document.getElementById('canvasContainer');
    const pipelineSelect = document.getElementById('pipelineSelect');
    const detailsDrawer = document.getElementById('detailsDrawer');
    const drawerTitle = document.getElementById('drawerTitle');
    const drawerBody = document.getElementById('drawerBody');
    const drawerClose = document.getElementById('drawerClose');
    const zoomLevelEl = document.getElementById('zoomLevel');

    // Populate Selector
    Object.keys(PIPELINES_DATA).sort().forEach(k => {
      const opt = document.createElement('option');
      opt.value = k;
      opt.textContent = k;
      if (k === 'master_pipeline') opt.selected = true;
      pipelineSelect.appendChild(opt);
    });

    if (PIPELINES_DATA['master_pipeline']) {
      currentPipelineKey = 'master_pipeline';
    }

    // Initialize Rough.js
    let rc = null;
    if (window.rough) {
      rc = rough.svg(roughSvg);
    }

    function updateTransform() {
      viewport.style.transform = `translate(${panX}px, ${panY}px) scale(${zoom})`;
      zoomLevelEl.textContent = `${Math.round(zoom * 100)}%`;
    }

    function getActivities() {
      const p = PIPELINES_DATA[currentPipelineKey];
      if (!p || !p.properties || !p.properties.activities) return [];
      return p.properties.activities;
    }

    // Auto-layout Algorithm (Topological rank / Sugiyama layered layout)
    function calculateLayout(activities) {
      const nodeMap = {};
      const inDegree = {};
      const adj = {};
      const positions = {};

      activities.forEach(a => {
        nodeMap[a.name] = a;
        inDegree[a.name] = 0;
        adj[a.name] = [];
      });

      activities.forEach(a => {
        (a.dependsOn || []).forEach(dep => {
          if (adj[dep.activity]) {
            adj[dep.activity].push(a.name);
            inDegree[a.name] = (inDegree[a.name] || 0) + 1;
          }
        });
      });

      // Layer assignment
      const layers = {};
      const queue = [];

      activities.forEach(a => {
        if ((inDegree[a.name] || 0) === 0) {
          queue.push({ name: a.name, layer: 0 });
          layers[a.name] = 0;
        }
      });

      while (queue.length > 0) {
        const curr = queue.shift();
        (adj[curr.name] || []).forEach(nxt => {
          const nextLayer = curr.layer + 1;
          if (layers[nxt] === undefined || layers[nxt] < nextLayer) {
            layers[nxt] = nextLayer;
            queue.push({ name: nxt, layer: nextLayer });
          }
        });
      }

      // Group nodes by layer
      const layerGroups = {};
      activities.forEach(a => {
        const l = layers[a.name] !== undefined ? layers[a.name] : 0;
        if (!layerGroups[l]) layerGroups[l] = [];
        layerGroups[l].push(a.name);
      });

      // Compute X, Y coordinates
      const LAYER_GAP_X = 350;
      const NODE_GAP_Y = 140;
      const START_X = 80;
      const START_Y = 80;

      Object.keys(layerGroups).forEach(lStr => {
        const l = parseInt(lStr, 10);
        const group = layerGroups[l];
        group.forEach((nodeName, idx) => {
          positions[nodeName] = {
            x: START_X + l * LAYER_GAP_X,
            y: START_Y + idx * NODE_GAP_Y,
            width: 250,
            height: 85
          };
        });
      });

      return positions;
    }

    // Render Canvas
    function render() {
      const activities = getActivities();
      nodesContainer.innerHTML = '';
      roughSvg.innerHTML = '';

      if (!rc && window.rough) {
        rc = rough.svg(roughSvg);
      }

      // Draw Edges (Arrows)
      activities.forEach(act => {
        const targetPos = nodePositions[act.name];
        if (!targetPos) return;

        (act.dependsOn || []).forEach(dep => {
          const sourcePos = nodePositions[dep.activity];
          if (!sourcePos) return;

          const startX = sourcePos.x + sourcePos.width;
          const startY = sourcePos.y + sourcePos.height / 2;
          const endX = targetPos.x;
          const endY = targetPos.y + targetPos.height / 2;

          const cond = (dep.dependencyConditions && dep.dependencyConditions[0]) || 'Succeeded';
          let strokeColor = '#2b6cb0'; // Blue on Succeeded
          if (cond === 'Failed') strokeColor = '#e53e3e';
          if (cond === 'Completed') strokeColor = '#718096';
          if (cond === 'Skipped') strokeColor = '#dd6b20';

          // Curved Bézier with Rough.js
          const midX = (startX + endX) / 2;
          const control1X = midX;
          const control1Y = startY;
          const control2X = midX;
          const control2Y = endY;

          const pathStr = `M ${startX} ${startY} C ${control1X} ${control1Y}, ${control2X} ${control2Y}, ${endX} ${endY}`;
          
          if (rc) {
            const curve = rc.path(pathStr, {
              stroke: strokeColor,
              strokeWidth: 2.2,
              roughness: 1.6,
              bowing: 1.4
            });
            roughSvg.appendChild(curve);

            // Arrowhead
            const arrowAngle = Math.atan2(endY - control2Y, endX - control2X);
            const arrowLen = 14;
            const aX1 = endX - arrowLen * Math.cos(arrowAngle - Math.PI / 6);
            const aY1 = endY - arrowLen * Math.sin(arrowAngle - Math.PI / 6);
            const aX2 = endX - arrowLen * Math.cos(arrowAngle + Math.PI / 6);
            const aY2 = endY - arrowLen * Math.sin(arrowAngle + Math.PI / 6);

            const arrowHead = rc.polygon([[endX, endY], [aX1, aY1], [aX2, aY2]], {
              fill: strokeColor,
              fillStyle: 'solid',
              stroke: strokeColor,
              roughness: 1.2
            });
            roughSvg.appendChild(arrowHead);
          }
        });
      });

      // Draw Nodes
      activities.forEach(act => {
        const pos = nodePositions[act.name];
        if (!pos) return;

        const meta = getTypeMeta(act.type);
        const nodeEl = document.createElement('div');
        nodeEl.className = `sketch-node ${act.name === selectedNodeName ? 'selected' : ''}`;
        nodeEl.style.left = `${pos.x}px`;
        nodeEl.style.top = `${pos.y}px`;
        nodeEl.style.width = `${pos.width}px`;

        // Check child activities for ForEach or IfCondition
        let subActivityCount = 0;
        if (act.type === 'ForEach' && act.typeProperties && act.typeProperties.activities) {
          subActivityCount = act.typeProperties.activities.length;
        } else if (act.type === 'IfCondition' && act.typeProperties) {
          const tCount = (act.typeProperties.ifTrueActivities || []).length;
          const fCount = (act.typeProperties.ifFalseActivities || []).length;
          subActivityCount = tCount + fCount;
        }

        nodeEl.innerHTML = `
          <div class="node-content">
            <div class="node-header">
              <span class="node-type-badge">${meta.label}</span>
            </div>
            <div class="node-name">${act.name}</div>
            ${act.description ? `<div class="node-desc">${act.description}</div>` : ''}
            ${subActivityCount > 0 ? `<div class="node-sub-count" onclick="openSubModal(event, '${act.name}')">🔍 View ${subActivityCount} inner activities</div>` : ''}
          </div>
        `;

        // Rough hand-drawn background card
        if (rc) {
          const rect = rc.rectangle(pos.x, pos.y, pos.width, pos.height, {
            fill: meta.fill,
            fillStyle: 'solid',
            stroke: '#2b2b2b',
            strokeWidth: 2,
            roughness: 1.8,
            bowing: 1.5
          });
          roughSvg.appendChild(rect);
        }

        // Click to view properties
        nodeEl.addEventListener('click', (e) => {
          e.stopPropagation();
          selectNode(act);
        });

        // Drag node
        nodeEl.addEventListener('mousedown', (e) => {
          if (e.target.closest('.node-sub-count')) return;
          e.stopPropagation();
          isDraggingNode = true;
          draggedNodeName = act.name;
          dragOffsetX = (e.clientX / zoom) - pos.x;
          dragOffsetY = (e.clientY / zoom) - pos.y;
        });

        nodesContainer.appendChild(nodeEl);
      });
    }

    function selectNode(act) {
      selectedNodeName = act.name;
      const meta = getTypeMeta(act.type);
      drawerTitle.textContent = act.name;

      let typePropsHtml = '';
      if (act.typeProperties) {
        const tp = act.typeProperties;
        if (tp.source) {
          typePropsHtml += `
            <div class="info-section">
              <div class="info-label">Source</div>
              <div class="info-value"><strong>Type:</strong> ${tp.source.type || 'N/A'}</div>
              ${tp.source.query ? `<div class="code-box">${escapeHtml(tp.source.query)}</div>` : ''}
              ${tp.source.sqlReaderQuery ? `<div class="code-box">${escapeHtml(tp.source.sqlReaderQuery)}</div>` : ''}
            </div>
          `;
        }
        if (tp.sink) {
          typePropsHtml += `
            <div class="info-section">
              <div class="info-label">Sink / Target</div>
              <div class="info-value"><strong>Type:</strong> ${tp.sink.type || 'N/A'}</div>
              ${tp.sink.tableActionOption ? `<div class="info-value"><strong>Action:</strong> ${tp.sink.tableActionOption}</div>` : ''}
            </div>
          `;
        }
        if (tp.storedProcedureName) {
          typePropsHtml += `
            <div class="info-section">
              <div class="info-label">Stored Procedure</div>
              <div class="code-box">${escapeHtml(tp.storedProcedureName)}</div>
            </div>
          `;
        }
        if (tp.notebookId) {
          typePropsHtml += `
            <div class="info-section">
              <div class="info-label">Notebook Artifact ID</div>
              <div class="info-value">${tp.notebookId}</div>
            </div>
          `;
        }
        if (tp.pipelineId) {
          typePropsHtml += `
            <div class="info-section">
              <div class="info-label">Invoked Pipeline ID</div>
              <div class="info-value">${tp.pipelineId}</div>
            </div>
          `;
        }
        if (tp.items) {
          typePropsHtml += `
            <div class="info-section">
              <div class="info-label">ForEach Items Expression</div>
              <div class="code-box">${escapeHtml(typeof tp.items === 'object' ? tp.items.value : tp.items)}</div>
            </div>
          `;
        }
        if (tp.expression) {
          typePropsHtml += `
            <div class="info-section">
              <div class="info-label">If Condition Expression</div>
              <div class="code-box">${escapeHtml(typeof tp.expression === 'object' ? tp.expression.value : tp.expression)}</div>
            </div>
          `;
        }
        if (tp.parameters) {
          typePropsHtml += `
            <div class="info-section">
              <div class="info-label">Parameters</div>
              <div class="code-box">${escapeHtml(JSON.stringify(tp.parameters, null, 2))}</div>
            </div>
          `;
        }
      }

      drawerBody.innerHTML = `
        <div class="info-section">
          <div class="info-label">Activity Type</div>
          <div><span class="badge-tag ${meta.class}">${meta.label} (${act.type})</span></div>
        </div>

        ${act.description ? `
          <div class="info-section">
            <div class="info-label">Description</div>
            <div class="info-value">${act.description}</div>
          </div>
        ` : ''}

        <div class="info-section">
          <div class="info-label">Dependencies</div>
          ${(act.dependsOn && act.dependsOn.length > 0) ? 
            act.dependsOn.map(d => `<div class="info-value">➔ <strong>${d.activity}</strong> (${(d.dependencyConditions || []).join(', ')})</div>`).join('') 
            : '<div class="info-value" style="color: var(--text-muted);">None (Entry point)</div>'}
        </div>

        ${typePropsHtml}

        <div class="info-section">
          <div class="info-label">Raw Activity JSON</div>
          <div class="code-box">${escapeHtml(JSON.stringify(act, null, 2))}</div>
        </div>
      `;

      detailsDrawer.classList.add('open');
      render();
    }

    function escapeHtml(str) {
      if (!str) return '';
      return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    // Switch Pipeline
    function loadPipeline(key) {
      currentPipelineKey = key;
      selectedNodeName = null;
      detailsDrawer.classList.remove('open');
      const activities = getActivities();
      nodePositions = calculateLayout(activities);
      centerView();
      render();
    }

    function centerView() {
      const activities = getActivities();
      if (activities.length === 0) return;
      let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
      Object.values(nodePositions).forEach(p => {
        minX = Math.min(minX, p.x);
        minY = Math.min(minY, p.y);
        maxX = Math.max(maxX, p.x + p.width);
        maxY = Math.max(maxY, p.y + p.height);
      });
      const cW = canvasContainer.clientWidth;
      const cH = canvasContainer.clientHeight;
      const gW = maxX - minX;
      const gH = maxY - minY;
      
      zoom = Math.min(1.0, Math.max(0.4, Math.min((cW - 140) / (gW || 1), (cH - 140) / (gH || 1))));
      panX = (cW - gW * zoom) / 2 - minX * zoom;
      panY = (cH - gH * zoom) / 2 - minY * zoom;
      updateTransform();
    }

    // Modal for Nested Activities
    window.openSubModal = function(e, parentName) {
      e.stopPropagation();
      const activities = getActivities();
      const act = activities.find(a => a.name === parentName);
      if (!act) return;

      const subModal = document.getElementById('subModal');
      const modalTitle = document.getElementById('modalTitle');
      const modalBody = document.getElementById('modalBody');

      modalTitle.textContent = `Inner Activities in: ${parentName} (${act.type})`;
      
      let innerActs = [];
      if (act.type === 'ForEach' && act.typeProperties) {
        innerActs = act.typeProperties.activities || [];
      } else if (act.type === 'IfCondition' && act.typeProperties) {
        const tActs = (act.typeProperties.ifTrueActivities || []).map(a => ({ ...a, _branch: 'If True (True Branch)' }));
        const fActs = (act.typeProperties.ifFalseActivities || []).map(a => ({ ...a, _branch: 'If False (False Branch)' }));
        innerActs = [...tActs, ...fActs];
      }

      if (innerActs.length === 0) {
        modalBody.innerHTML = '<p>No inner activities found.</p>';
      } else {
        modalBody.innerHTML = innerActs.map(ia => {
          const meta = getTypeMeta(ia.type);
          return `
            <div style="background: #f8fafc; border: 2px solid #2b2b2b; border-radius: 8px; padding: 14px; margin-bottom: 14px; box-shadow: 2px 2px 0px #2b2b2b;">
              <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 6px;">
                <span class="badge-tag ${meta.class}">${meta.label}</span>
                ${ia._branch ? `<span style="font-weight: 700; color: #b45309;">${ia._branch}</span>` : ''}
              </div>
              <h3 style="font-size: 18px; font-weight: 700; margin-bottom: 4px;">${ia.name}</h3>
              ${ia.description ? `<p style="color: #4b5563; font-size: 14px; margin-bottom: 6px;">${ia.description}</p>` : ''}
              <div class="code-box">${escapeHtml(JSON.stringify(ia, null, 2))}</div>
            </div>
          `;
        }).join('');
      }

      subModal.classList.add('open');
    };

    // Event Listeners
    pipelineSelect.addEventListener('change', (e) => {
      loadPipeline(e.target.value);
    });

    document.getElementById('btnAutoLayout').addEventListener('click', () => {
      nodePositions = calculateLayout(getActivities());
      centerView();
      render();
    });

    document.getElementById('btnResetView').addEventListener('click', () => {
      centerView();
      render();
    });

    document.getElementById('btnZoomIn').addEventListener('click', () => {
      zoom = Math.min(2.5, zoom + 0.15);
      updateTransform();
    });

    document.getElementById('btnZoomOut').addEventListener('click', () => {
      zoom = Math.max(0.2, zoom - 0.15);
      updateTransform();
    });

    drawerClose.addEventListener('click', () => {
      detailsDrawer.classList.remove('open');
      selectedNodeName = null;
      render();
    });

    document.getElementById('modalClose').addEventListener('click', () => {
      document.getElementById('subModal').classList.remove('open');
    });

    document.getElementById('subModal').addEventListener('click', (e) => {
      if (e.target.id === 'subModal') {
        document.getElementById('subModal').classList.remove('open');
      }
    });

    // Pan & Drag events on canvas
    canvasContainer.addEventListener('mousedown', (e) => {
      if (e.target.closest('.sketch-node') || e.target.closest('.top-navbar') || e.target.closest('.details-drawer')) return;
      isPanning = true;
      startPanX = e.clientX - panX;
      startPanY = e.clientY - panY;
    });

    window.addEventListener('mousemove', (e) => {
      if (isPanning) {
        panX = e.clientX - startPanX;
        panY = e.clientY - startPanY;
        updateTransform();
      } else if (isDraggingNode && draggedNodeName && nodePositions[draggedNodeName]) {
        nodePositions[draggedNodeName].x = (e.clientX / zoom) - dragOffsetX;
        nodePositions[draggedNodeName].y = (e.clientY / zoom) - dragOffsetY;
        render();
      }
    });

    window.addEventListener('mouseup', () => {
      isPanning = false;
      isDraggingNode = false;
      draggedNodeName = null;
    });

    canvasContainer.addEventListener('wheel', (e) => {
      e.preventDefault();
      const zoomFactor = e.deltaY < 0 ? 1.08 : 0.92;
      const newZoom = Math.min(2.5, Math.max(0.2, zoom * zoomFactor));
      
      const rect = canvasContainer.getBoundingClientRect();
      const mouseX = e.clientX - rect.left;
      const mouseY = e.clientY - rect.top;

      panX = mouseX - (mouseX - panX) * (newZoom / zoom);
      panY = mouseY - (mouseY - panY) * (newZoom / zoom);
      zoom = newZoom;

      updateTransform();
    }, { passive: false });

    // Initial Start
    loadPipeline(currentPipelineKey);
  </script>
</body>
</html>
"""

def generate_html():
    data = collect_pipelines()
    json_str = json.dumps(data, indent=2, ensure_ascii=False)
    html_content = HTML_TEMPLATE.replace("__PIPELINES_DATA_PLACEHOLDER__", json_str)
    
    out_file = "pipeline_visualizer.html"
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"Generated {out_file} successfully ({len(html_content)} bytes)")

if __name__ == "__main__":
    generate_html()
