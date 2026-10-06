import os
import re

svg_files = [
    ("docs/00-architecture-overview/diagram_1_tech_stack.svg", "tab-stack", "viewer-stack", "1"),
    ("docs/00-architecture-overview/diagram_2_pipeline_flow.svg", "tab-pipeline", "viewer-pipeline", "2"),
    ("docs/00-architecture-overview/diagram_3_sequence_flow.svg", "tab-sequence", "viewer-sequence", "3"),
    ("docs/00-architecture-overview/diagram_4_plug_and_play.svg", "tab-boundary", "viewer-boundary", "4")
]

inlined_svgs = {}

for filepath, tab_id, viewer_id, idx in svg_files:
    if not os.path.exists(filepath):
        alt_path = filepath.replace("docs/00-architecture-overview/", "docs/")
        if os.path.exists(alt_path):
            filepath = alt_path
        else:
            raise FileNotFoundError(f"Missing {filepath}")
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Scope IDs and CSS selectors
    content = content.replace('id="mermaid-svg"', f'id="mermaid-svg-{idx}"')
    content = content.replace('#mermaid-svg', f'#mermaid-svg-{idx}')
    
    # Remove external font-awesome import
    content = re.sub(r'<style xmlns="http://www.w3.org/1999/xhtml">@import url\("[^"]+"\);</style>', '', content)
    
    # Replace style="max-width: ...;" with style="max-width: none; overflow: visible;"
    content = re.sub(r'style="max-width:[^"]*"', 'style="max-width: none !important; overflow: visible !important;"', content)
    
    inlined_svgs[tab_id] = content
    print(f"Processed {filepath}: length={len(content)}")

html_template = f"""<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Hệ Thống Streaming MLOps E-Commerce - Kiến Trúc & Tech Stack</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg-primary: #060911;
            --bg-secondary: #0f172a;
            --bg-card: rgba(15, 23, 42, 0.85);
            --border-color: rgba(255, 255, 255, 0.1);
            --accent-blue: #38bdf8;
            --accent-green: #34d399;
            --accent-amber: #f59e0b;
            --accent-pink: #ec4899;
            --accent-purple: #a855f7;
            --accent-cyan: #06b6d4;
            --text-main: #f8fafc;
            --text-muted: #94a3b8;
            --glass-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.8);
        }}

        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}

        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: radial-gradient(circle at 10% 20%, rgba(56, 189, 248, 0.08) 0%, transparent 40%),
                        radial-gradient(circle at 90% 80%, rgba(168, 85, 247, 0.08) 0%, transparent 40%),
                        var(--bg-primary);
            color: var(--text-main);
            min-height: 100vh;
            padding: 1.5rem;
            line-height: 1.6;
        }}

        .container {{
            max-width: 1680px;
            margin: 0 auto;
        }}

        header {{
            margin-bottom: 1.75rem;
            padding-bottom: 1.25rem;
            border-bottom: 1px solid var(--border-color);
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            flex-wrap: wrap;
            gap: 1.25rem;
        }}

        .header-title h1 {{
            font-size: 2.1rem;
            font-weight: 800;
            letter-spacing: -0.03em;
            background: linear-gradient(135deg, #38bdf8 0%, #818cf8 50%, #c084fc 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 0.4rem;
        }}

        .header-title p {{
            color: var(--text-muted);
            font-size: 0.98rem;
            max-width: 950px;
        }}

        .badge-group {{
            display: flex;
            gap: 0.5rem;
            flex-wrap: wrap;
            margin-top: 0.75rem;
        }}

        .badge {{
            font-size: 0.78rem;
            padding: 0.3rem 0.75rem;
            border-radius: 9999px;
            background: rgba(56, 189, 248, 0.12);
            color: var(--accent-blue);
            border: 1px solid rgba(56, 189, 248, 0.3);
            font-weight: 600;
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
        }}

        .badge.green {{
            background: rgba(52, 211, 153, 0.12);
            color: var(--accent-green);
            border-color: rgba(52, 211, 153, 0.3);
        }}

        .badge.purple {{
            background: rgba(168, 85, 247, 0.12);
            color: var(--accent-purple);
            border-color: rgba(168, 85, 247, 0.3);
        }}

        /* Navigation Tabs */
        .tab-bar {{
            display: flex;
            gap: 0.6rem;
            background: rgba(15, 23, 42, 0.95);
            padding: 0.5rem;
            border-radius: 14px;
            border: 1px solid var(--border-color);
            margin-bottom: 1.5rem;
            overflow-x: auto;
            backdrop-filter: blur(16px);
            box-shadow: 0 8px 24px rgba(0, 0, 0, 0.5);
        }}

        .tab-btn {{
            background: transparent;
            border: 1px solid transparent;
            color: var(--text-muted);
            font-family: inherit;
            font-size: 0.92rem;
            font-weight: 600;
            padding: 0.65rem 1.25rem;
            border-radius: 10px;
            cursor: pointer;
            transition: all 0.2s ease;
            white-space: nowrap;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}

        .tab-btn:hover {{
            color: var(--text-main);
            background: rgba(255, 255, 255, 0.05);
        }}

        .tab-btn.active {{
            background: linear-gradient(135deg, rgba(56, 189, 248, 0.2), rgba(129, 140, 248, 0.25));
            color: #ffffff;
            border: 1px solid rgba(56, 189, 248, 0.5);
            box-shadow: 0 4px 16px rgba(56, 189, 248, 0.25);
        }}

        /* Tab Contents */
        .tab-panel {{
            display: none;
            animation: fadeIn 0.2s ease-out;
        }}

        .tab-panel.active {{
            display: block;
        }}

        @keyframes fadeIn {{
            from {{ opacity: 0; transform: translateY(4px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}

        .card {{
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 18px;
            padding: 1.75rem;
            backdrop-filter: blur(20px);
            box-shadow: var(--glass-shadow);
            margin-bottom: 2rem;
        }}

        .card-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 0.75rem;
            padding-bottom: 0.75rem;
            border-bottom: 1px solid var(--border-color);
            flex-wrap: wrap;
            gap: 0.75rem;
        }}

        .card-header h2 {{
            font-size: 1.35rem;
            font-weight: 700;
            color: var(--text-main);
            display: flex;
            align-items: center;
            gap: 0.6rem;
        }}

        .card-desc {{
            color: var(--text-muted);
            font-size: 0.95rem;
            margin-bottom: 1.25rem;
        }}

        /* Interactive Diagram Canvas Component */
        .diagram-wrapper {{
            position: relative;
            background: #060911;
            border-radius: 14px;
            border: 1px solid var(--border-color);
            box-shadow: inset 0 2px 8px rgba(0, 0, 0, 0.8);
            overflow: hidden;
        }}

        .diagram-wrapper:fullscreen {{
            border-radius: 0;
            padding: 0;
            background: #05070e;
        }}

        .diagram-toolbar {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            background: rgba(15, 23, 42, 0.98);
            padding: 0.65rem 1rem;
            border-bottom: 1px solid var(--border-color);
            flex-wrap: wrap;
            gap: 0.75rem;
            z-index: 10;
            position: relative;
            backdrop-filter: blur(12px);
        }}

        .toolbar-group {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
            flex-wrap: wrap;
        }}

        .btn-tool {{
            background: rgba(255, 255, 255, 0.06);
            border: 1px solid rgba(255, 255, 255, 0.12);
            color: var(--text-main);
            padding: 0.4rem 0.8rem;
            border-radius: 6px;
            font-family: inherit;
            font-size: 0.84rem;
            font-weight: 600;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
            transition: all 0.15s ease;
            text-decoration: none;
        }}

        .btn-tool:hover {{
            background: rgba(56, 189, 248, 0.15);
            border-color: rgba(56, 189, 248, 0.4);
            color: var(--accent-blue);
        }}

        .btn-tool:active {{
            transform: scale(0.96);
        }}

        .btn-tool.accent {{
            background: rgba(56, 189, 248, 0.18);
            border-color: rgba(56, 189, 248, 0.4);
            color: #38bdf8;
        }}

        .zoom-slider-container {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
            background: rgba(0, 0, 0, 0.35);
            padding: 0.25rem 0.65rem;
            border-radius: 6px;
            border: 1px solid rgba(255, 255, 255, 0.08);
        }}

        .zoom-slider {{
            -webkit-appearance: none;
            width: 120px;
            height: 4px;
            background: rgba(255, 255, 255, 0.2);
            border-radius: 2px;
            outline: none;
            cursor: pointer;
        }}

        .zoom-slider::-webkit-slider-thumb {{
            -webkit-appearance: none;
            width: 14px;
            height: 14px;
            background: var(--accent-blue);
            border-radius: 50%;
            cursor: pointer;
            box-shadow: 0 0 6px rgba(56, 189, 248, 0.8);
        }}

        .zoom-badge {{
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.82rem;
            color: var(--accent-blue);
            min-width: 44px;
            text-align: right;
            font-weight: 600;
        }}

        .hint-bar {{
            background: rgba(56, 189, 248, 0.05);
            padding: 0.4rem 1rem;
            font-size: 0.78rem;
            color: #94a3b8;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            display: flex;
            align-items: center;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 0.5rem;
        }}

        .diagram-viewport {{
            width: 100%;
            height: 750px;
            overflow: hidden;
            position: relative;
            cursor: grab;
            background-color: #060911;
            background-image: radial-gradient(rgba(255, 255, 255, 0.1) 1.2px, transparent 1.2px);
            background-size: 24px 24px;
            user-select: none;
        }}

        .diagram-wrapper:fullscreen .diagram-viewport {{
            height: calc(100vh - 85px);
        }}

        .diagram-viewport:active {{
            cursor: grabbing;
        }}

        .diagram-canvas {{
            position: absolute;
            transform-origin: 0 0;
            will-change: transform;
            display: inline-block;
        }}

        .diagram-canvas svg {{
            display: block !important;
            max-width: none !important;
            height: auto !important;
            overflow: visible !important;
        }}

        /* Callout Box */
        .callout {{
            border-left: 4px solid var(--accent-blue);
            background: rgba(56, 189, 248, 0.06);
            padding: 1.25rem;
            border-radius: 0 10px 10px 0;
            margin-top: 1.5rem;
        }}

        .callout-title {{
            font-weight: 700;
            color: var(--accent-blue);
            margin-bottom: 0.35rem;
            display: flex;
            align-items: center;
            gap: 0.4rem;
        }}

        /* Table Styling */
        .table-responsive {{
            overflow-x: auto;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
            text-align: left;
        }}

        th {{
            background: rgba(255, 255, 255, 0.04);
            color: var(--accent-blue);
            font-weight: 700;
            text-transform: uppercase;
            font-size: 0.78rem;
            letter-spacing: 0.05em;
            padding: 1rem;
            border-bottom: 1px solid var(--border-color);
        }}

        td {{
            padding: 1rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.04);
            color: #cbd5e1;
        }}

        tr:hover td {{
            background: rgba(255, 255, 255, 0.02);
            color: #fff;
        }}

        code {{
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.85em;
            background: rgba(255, 255, 255, 0.06);
            padding: 0.15rem 0.4rem;
            border-radius: 4px;
            color: var(--accent-green);
        }}

        footer {{
            text-align: center;
            color: var(--text-muted);
            font-size: 0.85rem;
            padding: 2.5rem 0;
            border-top: 1px solid var(--border-color);
            margin-top: 3rem;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="header-title">
                <h1>Hệ Thống Streaming MLOps E-Commerce</h1>
                <p>Kiến trúc Streaming & Machine Learning Operations phục vụ Dự báo Nhu cầu & Tối ưu Quản trị Tồn kho Động Đa Sàn (Shopee 84 cột & TikTok Shop 71 cột).</p>
                <div class="badge-group">
                    <span class="badge">🚀 6 Phân Tầng Kiến Trúc</span>
                    <span class="badge green">⚡ Source-Agnostic Plug & Play</span>
                    <span class="badge purple">🔄 Closed-Loop Drift Retraining</span>
                </div>
            </div>
            <div style="display: flex; gap: 0.6rem; align-items: center;">
                <button class="btn-tool" onclick="window.print()">
                    🖨️ In / Xuất PDF
                </button>
            </div>
        </header>

        <!-- Navigation Tabs -->
        <nav class="tab-bar">
            <button class="tab-btn active" onclick="switchTab('tab-stack')">🛠️ 1. Tech Stack Taxonomy</button>
            <button class="tab-btn" onclick="switchTab('tab-pipeline')">🔄 2. End-to-End Data Pipeline</button>
            <button class="tab-btn" onclick="switchTab('tab-sequence')">⏱️ 3. Real-Time Sequence Flow</button>
            <button class="tab-btn" onclick="switchTab('tab-boundary')">🔌 4. Ranh Giới "Cắm-Rút" Dữ Liệu</button>
            <button class="tab-btn" onclick="switchTab('tab-matrix')">📊 5. Ma Trận 16 Công Nghệ</button>
        </nav>

        <!-- ==================== TAB 1: TECH STACK TAXONOMY ==================== -->
        <section id="tab-stack" class="tab-panel active">
            <div class="card">
                <div class="card-header">
                    <h2>🛠️ Phân Tầng Công Nghệ Toàn Hệ Thống (6 Tiers - 9 Microservices)</h2>
                    <span class="badge">Distributed Cloud-Native Architecture</span>
                </div>
                <p class="card-desc">Sơ đồ tổng quan toàn bộ 16 công nghệ cốt lõi và các microservices liên thông trong hệ thống:</p>

                <div class="diagram-wrapper" id="viewer-stack">
                    <div class="diagram-toolbar">
                        <div class="toolbar-group">
                            <button class="btn-tool" onclick="viewers['tab-stack'].zoomOut()">🔍 -</button>
                            <div class="zoom-slider-container">
                                <input type="range" min="30" max="300" value="100" class="zoom-slider" oninput="viewers['tab-stack'].onSliderChange(this.value)">
                                <span class="zoom-badge">100%</span>
                            </div>
                            <button class="btn-tool" onclick="viewers['tab-stack'].zoomIn()">🔍 +</button>
                            <button class="btn-tool" onclick="viewers['tab-stack'].resetView()">↺ 100% (Gốc)</button>
                            <button class="btn-tool" onclick="viewers['tab-stack'].fitView()">🎯 Vừa Khung</button>
                        </div>
                        <div class="toolbar-group">
                            <a href="diagram_1_tech_stack.svg" target="_blank" class="btn-tool accent">🔗 Mở ảnh gốc (.SVG)</a>
                            <button class="btn-tool" onclick="viewers['tab-stack'].toggleFullscreen()">⛶ Toàn Màn Hình</button>
                        </div>
                    </div>
                    <div class="hint-bar">
                        <span>💡 <strong>Hướng dẫn</strong>: Giữ chuột trái & kéo rê (Pan) để di chuyển • Lăn chuột (Scroll) để phóng to/thu nhỏ • Bấm "Toàn Màn Hình" để mở rộng</span>
                        <span style="color: var(--accent-blue);">Vector SVG Nhúng Trực Tiếp (Sắc nét 100%)</span>
                    </div>
                    <div class="diagram-viewport">
                        <div class="diagram-canvas">
                            {inlined_svgs['tab-stack']}
                        </div>
                    </div>
                </div>
            </div>
        </section>

        <!-- ==================== TAB 2: DATA PIPELINE FLOW ==================== -->
        <section id="tab-pipeline" class="tab-panel">
            <div class="card">
                <div class="card-header">
                    <h2>🔄 Sơ Đồ Luồng Hoạt Động Chi Tiết (End-to-End Pipeline)</h2>
                    <span class="badge green">Real-Time Event Processing</span>
                </div>
                <p class="card-desc">Hành trình dữ liệu từ đơn hàng thô đến suy diễn máy học và kích hoạt vòng phản hồi thích ứng:</p>

                <div class="diagram-wrapper" id="viewer-pipeline">
                    <div class="diagram-toolbar">
                        <div class="toolbar-group">
                            <button class="btn-tool" onclick="viewers['tab-pipeline'].zoomOut()">🔍 -</button>
                            <div class="zoom-slider-container">
                                <input type="range" min="30" max="300" value="100" class="zoom-slider" oninput="viewers['tab-pipeline'].onSliderChange(this.value)">
                                <span class="zoom-badge">100%</span>
                            </div>
                            <button class="btn-tool" onclick="viewers['tab-pipeline'].zoomIn()">🔍 +</button>
                            <button class="btn-tool" onclick="viewers['tab-pipeline'].resetView()">↺ 100% (Gốc)</button>
                            <button class="btn-tool" onclick="viewers['tab-pipeline'].fitView()">🎯 Vừa Khung</button>
                        </div>
                        <div class="toolbar-group">
                            <a href="diagram_2_pipeline_flow.svg" target="_blank" class="btn-tool accent">🔗 Mở ảnh gốc (.SVG)</a>
                            <button class="btn-tool" onclick="viewers['tab-pipeline'].toggleFullscreen()">⛶ Toàn Màn Hình</button>
                        </div>
                    </div>
                    <div class="hint-bar">
                        <span>💡 <strong>Hướng dẫn</strong>: Giữ chuột trái & kéo rê (Pan) để di chuyển • Lăn chuột (Scroll) để phóng to/thu nhỏ • Bấm "Toàn Màn Hình" để mở rộng</span>
                        <span style="color: var(--accent-green);">Đơn hàng Shopee 84 cols & TikTok 71 cols</span>
                    </div>
                    <div class="diagram-viewport">
                        <div class="diagram-canvas">
                            {inlined_svgs['tab-pipeline']}
                        </div>
                    </div>
                </div>
            </div>
        </section>

        <!-- ==================== TAB 3: SEQUENCE FLOW ==================== -->
        <section id="tab-sequence" class="tab-panel">
            <div class="card">
                <div class="card-header">
                    <h2>⏱️ Sơ Đồ Tuần Tự Xử Lý Sự Kiện & Tự Động Thích Ứng</h2>
                    <span class="badge purple">Zero-Downtime Hot Reload</span>
                </div>
                <p class="card-desc">Sự tương tác giữa các thành phần theo trục thời gian thực (từ đơn hàng đến cảnh báo và tự kích hoạt train lại):</p>

                <div class="diagram-wrapper" id="viewer-sequence">
                    <div class="diagram-toolbar">
                        <div class="toolbar-group">
                            <button class="btn-tool" onclick="viewers['tab-sequence'].zoomOut()">🔍 -</button>
                            <div class="zoom-slider-container">
                                <input type="range" min="30" max="300" value="100" class="zoom-slider" oninput="viewers['tab-sequence'].onSliderChange(this.value)">
                                <span class="zoom-badge">100%</span>
                            </div>
                            <button class="btn-tool" onclick="viewers['tab-sequence'].zoomIn()">🔍 +</button>
                            <button class="btn-tool" onclick="viewers['tab-sequence'].resetView()">↺ 100% (Gốc)</button>
                            <button class="btn-tool" onclick="viewers['tab-sequence'].fitView()">🎯 Vừa Khung</button>
                        </div>
                        <div class="toolbar-group">
                            <a href="diagram_3_sequence_flow.svg" target="_blank" class="btn-tool accent">🔗 Mở ảnh gốc (.SVG)</a>
                            <button class="btn-tool" onclick="viewers['tab-sequence'].toggleFullscreen()">⛶ Toàn Màn Hình</button>
                        </div>
                    </div>
                    <div class="hint-bar">
                        <span>💡 <strong>Hướng dẫn</strong>: Giữ chuột trái & kéo rê (Pan) để di chuyển • Lăn chuột (Scroll) để phóng to/thu nhỏ • Bấm "Toàn Màn Hình" để mở rộng</span>
                        <span style="color: var(--accent-purple);">Asynchronous Event Cycle</span>
                    </div>
                    <div class="diagram-viewport">
                        <div class="diagram-canvas">
                            {inlined_svgs['tab-sequence']}
                        </div>
                    </div>
                </div>
            </div>
        </section>

        <!-- ==================== TAB 4: PLUG AND PLAY BOUNDARY ==================== -->
        <section id="tab-boundary" class="tab-panel">
            <div class="card">
                <div class="card-header">
                    <h2>🔌 Ranh Giới "Cắm-Rút" Dữ Liệu Thật (Plug-and-Play Contract Boundary)</h2>
                    <span class="badge green">Zero Breaking Changes</span>
                </div>
                <p class="card-desc">Minh chứng cho thiết kế: khi có dữ liệu thật từ doanh nghiệp, chỉ cần thay đổi nguồn nạp ở đầu vào, 100% phần còn lại của hệ thống không cần sửa đổi:</p>

                <div class="diagram-wrapper" id="viewer-boundary">
                    <div class="diagram-toolbar">
                        <div class="toolbar-group">
                            <button class="btn-tool" onclick="viewers['tab-boundary'].zoomOut()">🔍 -</button>
                            <div class="zoom-slider-container">
                                <input type="range" min="30" max="300" value="100" class="zoom-slider" oninput="viewers['tab-boundary'].onSliderChange(this.value)">
                                <span class="zoom-badge">100%</span>
                            </div>
                            <button class="btn-tool" onclick="viewers['tab-boundary'].zoomIn()">🔍 +</button>
                            <button class="btn-tool" onclick="viewers['tab-boundary'].resetView()">↺ 100% (Gốc)</button>
                            <button class="btn-tool" onclick="viewers['tab-boundary'].fitView()">🎯 Vừa Khung</button>
                        </div>
                        <div class="toolbar-group">
                            <a href="diagram_4_plug_and_play.svg" target="_blank" class="btn-tool accent">🔗 Mở ảnh gốc (.SVG)</a>
                            <button class="btn-tool" onclick="viewers['tab-boundary'].toggleFullscreen()">⛶ Toàn Màn Hình</button>
                        </div>
                    </div>
                    <div class="hint-bar">
                        <span>💡 <strong>Hướng dẫn</strong>: Giữ chuột trái & kéo rê (Pan) để di chuyển • Lăn chuột (Scroll) để phóng to/thu nhỏ • Bấm "Toàn Màn Hình" để mở rộng</span>
                        <span style="color: var(--accent-green);">Độc lập nguồn nạp (Source-Agnostic)</span>
                    </div>
                    <div class="diagram-viewport">
                        <div class="diagram-canvas">
                            {inlined_svgs['tab-boundary']}
                        </div>
                    </div>
                </div>

                <div class="callout">
                    <div class="callout-title">💡 Giải Thích Kỹ Thuật Bảo Vệ Đồ Án Tốt Nghiệp:</div>
                    <p>Hệ thống sử dụng nguyên lý <strong>Giao ước Dữ liệu (Data Contract)</strong>: Toàn bộ cấu trúc đơn hàng của bộ mô phỏng <code>data_simulator.py</code> được định nghĩa chính xác theo chuẩn API Webhook và Export của Shopee (84 cột) và TikTok Shop (71 cột). Do đó, tầng trích xuất <code>unify_schema()</code> và kho <code>Fact_Orders</code> đã tương thích 100% với dữ liệu thực tế mà không cần chỉnh sửa bất kỳ dòng code xử lý nào.</p>
                </div>
            </div>
        </section>

        <!-- ==================== TAB 5: TECH STACK MATRIX ==================== -->
        <section id="tab-matrix" class="tab-panel">
            <div class="card">
                <div class="card-header">
                    <h2>📊 Bảng Ma Trận Công Nghệ Chi Tiết (16 Components)</h2>
                    <span class="badge">Production Specifications</span>
                </div>
                <div class="table-responsive">
                    <table>
                        <thead>
                            <tr>
                                <th>Phân tầng</th>
                                <th>Công nghệ / Thư viện</th>
                                <th>Phiên bản</th>
                                <th>Trách nhiệm chuyên biệt</th>
                                <th>Giao thức / Cổng</th>
                                <th>Định dạng dữ liệu</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td><strong>Ingestion Broker</strong></td>
                                <td><strong>Apache Kafka</strong></td>
                                <td><code>7.6.0</code></td>
                                <td>Hàng đợi tin nhắn phân tán, đệm luồng đơn hàng tốc độ cao</td>
                                <td>TCP: <code>9092, 29092</code></td>
                                <td>Binary JSON Payload</td>
                            </tr>
                            <tr>
                                <td><strong>Coordinator</strong></td>
                                <td><strong>Apache Zookeeper</strong></td>
                                <td><code>7.6.0</code></td>
                                <td>Quản lý đồng thuận cluster và offset phân vùng</td>
                                <td>TCP: <code>2181</code></td>
                                <td>Nội bộ Cluster</td>
                            </tr>
                            <tr>
                                <td><strong>Data Lake</strong></td>
                                <td><strong>MinIO Object Storage</strong></td>
                                <td><code>RELEASE.2024</code></td>
                                <td>Hồ chứa dữ liệu thô chuẩn S3 bất biến (Bronze Layer)</td>
                                <td>HTTP: <code>9000, 9001</code></td>
                                <td>Apache Parquet (Snappy)</td>
                            </tr>
                            <tr>
                                <td><strong>Data Warehouse</strong></td>
                                <td><strong>PostgreSQL</strong></td>
                                <td><code>16.2</code></td>
                                <td>Kho dữ liệu quan hệ mô hình Star Schema (1 Fact + 5 Dim)</td>
                                <td>TCP: <code>5432</code></td>
                                <td>SQL Tabular (VND)</td>
                            </tr>
                            <tr>
                                <td><strong>ETL & Data Quality</strong></td>
                                <td><strong>Pandas & SQLAlchemy</strong></td>
                                <td><code>2.2 / 2.0</code></td>
                                <td>Làm sạch, chuẩn hóa lược đồ Shopee/TikTok, kiểm tra ràng buộc</td>
                                <td>Python In-Process</td>
                                <td>DataFrames / Dicts</td>
                            </tr>
                            <tr>
                                <td><strong>ML Lifecycle</strong></td>
                                <td><strong>MLflow</strong></td>
                                <td><code>2.15.0</code></td>
                                <td>Theo dõi thử nghiệm, log siêu tham số và quản trị Model Registry</td>
                                <td>HTTP: <code>5000</code></td>
                                <td>Pickle / ONNX / PyFunc</td>
                            </tr>
                            <tr>
                                <td><strong>Gradient Boosting</strong></td>
                                <td><strong>LightGBM & XGBoost</strong></td>
                                <td><code>4.5 / 2.1</code></td>
                                <td>Thuật toán Machine Learning cốt lõi dự báo nhu cầu (Champion)</td>
                                <td>Python In-Process</td>
                                <td>Scikit-Learn Estimator</td>
                            </tr>
                            <tr>
                                <td><strong>Deep Learning</strong></td>
                                <td><strong>PyTorch</strong></td>
                                <td><code>2.4.0</code></td>
                                <td>Mạng nơ-ron hồi quy tuần tự sâu (Deep LSTM & GRU 2 lớp)</td>
                                <td>Python In-Process</td>
                                <td>Tensor Matrices</td>
                            </tr>
                            <tr>
                                <td><strong>Hyperparameter</strong></td>
                                <td><strong>Optuna</strong></td>
                                <td><code>3.6.1</code></td>
                                <td>Tối ưu siêu tham số tự động thuật toán Bayesian Optimization</td>
                                <td>Python In-Process</td>
                                <td>Trial History</td>
                            </tr>
                            <tr>
                                <td><strong>Model Serving</strong></td>
                                <td><strong>FastAPI & Uvicorn</strong></td>
                                <td><code>0.115</code></td>
                                <td>Cung cấp REST endpoints dự báo và cảnh báo tồn kho (< 2ms)</td>
                                <td>HTTP: <code>8000</code></td>
                                <td>REST JSON / OpenAPI 3.1</td>
                            </tr>
                            <tr>
                                <td><strong>Inventory Optimizer</strong></td>
                                <td><strong>Dynamic Stochastic Engine</strong></td>
                                <td><code>Custom</code></td>
                                <td>Tính toán Safety Stock (SS) và Reorder Point (ROP) theo dịch vụ Z</td>
                                <td>Python In-Process</td>
                                <td>Pydantic Models</td>
                            </tr>
                            <tr>
                                <td><strong>System Metric Scraper</strong></td>
                                <td><strong>Prometheus</strong></td>
                                <td><code>2.53.0</code></td>
                                <td>Thu thập chỉ số hoạt động thời gian thực từ serving service</td>
                                <td>HTTP: <code>9090</code></td>
                                <td>Metrics text v0.0.4</td>
                            </tr>
                            <tr>
                                <td><strong>MLOps Dashboards</strong></td>
                                <td><strong>Grafana</strong></td>
                                <td><code>11.1.0</code></td>
                                <td>Bảng điều khiển trực quan hóa thời gian thực giám sát thông lượng</td>
                                <td>HTTP: <code>3000</code></td>
                                <td>JSON Dashboards</td>
                            </tr>
                            <tr>
                                <td><strong>Drift Detection</strong></td>
                                <td><strong>Evidently AI</strong></td>
                                <td><code>0.4.30</code></td>
                                <td>Kiểm định thống kê 2 mẫu KS-test, đo lường trôi dạt PSI</td>
                                <td>Python In-Process</td>
                                <td>HTML Reports / JSON</td>
                            </tr>
                            <tr>
                                <td><strong>Executive Analytics</strong></td>
                                <td><strong>Power BI Desktop</strong></td>
                                <td><code>2024</code></td>
                                <td>Dashboard báo cáo điều hành 4 trang (Doanh số, ABC/XYZ, Tồn kho)</td>
                                <td>Power BI Engine</td>
                                <td>Tabular Model / DAX</td>
                            </tr>
                            <tr>
                                <td><strong>Stress Testing</strong></td>
                                <td><strong>Locust</strong></td>
                                <td><code>2.31.0</code></td>
                                <td>Giả lập tải đồng thời 10-200 người dùng, kiểm tra điểm nghẽn</td>
                                <td>HTTP: <code>8089</code></td>
                                <td>HTTP Benchmark Stats</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </section>

        <footer>
            Dự án Tốt nghiệp: Hệ thống Streaming MLOps E-Commerce (Shopee & TikTok Shop) • Thiết kế bởi Nolan Minh Dinh
        </footer>
    </div>

    <script>
        class InteractiveSvgViewer {{
            constructor(wrapperId) {{
                this.wrapper = document.getElementById(wrapperId);
                this.viewport = this.wrapper.querySelector('.diagram-viewport');
                this.canvas = this.wrapper.querySelector('.diagram-canvas');
                this.svg = this.canvas.querySelector('svg');
                this.slider = this.wrapper.querySelector('.zoom-slider');
                this.badge = this.wrapper.querySelector('.zoom-badge');
                
                this.scale = 1.0;
                this.x = 20;
                this.y = 20;
                this.isDragging = false;
                this.startX = 0;
                this.startY = 0;

                // Extract exact natural dimensions from viewBox
                this.svgW = 1600;
                this.svgH = 2000;
                if (this.svg) {{
                    const vb = this.svg.getAttribute('viewBox');
                    if (vb) {{
                        const parts = vb.split(/[ ,]+/).map(Number);
                        if (parts.length >= 4) {{
                            this.svgW = parts[2];
                            this.svgH = parts[3];
                        }}
                    }}
                }}

                this.initPanZoom();
                setTimeout(() => {{
                    this.fitView();
                }}, 50);
            }}

            initPanZoom() {{
                // Kéo rê chuột (Drag to Pan)
                this.viewport.addEventListener('mousedown', (e) => {{
                    if (e.button !== 0) return;
                    this.isDragging = true;
                    this.startX = e.clientX - this.x;
                    this.startY = e.clientY - this.y;
                    this.viewport.style.cursor = 'grabbing';
                }});

                window.addEventListener('mousemove', (e) => {{
                    if (!this.isDragging) return;
                    this.x = e.clientX - this.startX;
                    this.y = e.clientY - this.startY;
                    this.updateTransform();
                }});

                window.addEventListener('mouseup', () => {{
                    if (this.isDragging) {{
                        this.isDragging = false;
                        this.viewport.style.cursor = 'grab';
                    }}
                }});

                // Lăn chuột phóng to / thu nhỏ tại vị trí con trỏ (Mouse Wheel Zoom)
                this.viewport.addEventListener('wheel', (e) => {{
                    e.preventDefault();
                    const rect = this.viewport.getBoundingClientRect();
                    const pointerX = e.clientX - rect.left;
                    const pointerY = e.clientY - rect.top;

                    const zoomFactor = e.deltaY < 0 ? 1.15 : 0.85;
                    this.zoomAt(this.scale * zoomFactor, pointerX, pointerY);
                }}, {{ passive: false }});
            }}

            zoomAt(newScale, pointX, pointY) {{
                newScale = Math.max(0.25, Math.min(3.5, newScale));
                const ratio = newScale / this.scale;
                this.x = pointX - (pointX - this.x) * ratio;
                this.y = pointY - (pointY - this.y) * ratio;
                this.scale = newScale;
                this.updateTransform();
            }}

            zoomIn() {{
                const rect = this.viewport.getBoundingClientRect();
                this.zoomAt(this.scale * 1.25, rect.width / 2, rect.height / 2);
            }}

            zoomOut() {{
                const rect = this.viewport.getBoundingClientRect();
                this.zoomAt(this.scale * 0.8, rect.width / 2, rect.height / 2);
            }}

            onSliderChange(val) {{
                const targetScale = parseInt(val) / 100;
                const rect = this.viewport.getBoundingClientRect();
                this.zoomAt(targetScale, rect.width / 2, rect.height / 2);
            }}

            resetView() {{
                this.scale = 1.0;
                const vpW = this.viewport.clientWidth;
                this.x = Math.max(20, (vpW - this.svgW) / 2);
                this.y = 20;
                this.updateTransform();
            }}

            fitView() {{
                const vpW = this.viewport.clientWidth;
                const vpH = this.viewport.clientHeight;

                const pad = 40;
                const scaleX = (vpW - pad * 2) / this.svgW;
                const scaleY = (vpH - pad * 2) / this.svgH;
                
                let fitScale = Math.min(scaleX, scaleY);
                fitScale = Math.max(0.35, Math.min(1.2, fitScale));

                this.scale = fitScale;
                this.x = Math.max(20, (vpW - this.svgW * fitScale) / 2);
                this.y = Math.max(20, (vpH - this.svgH * fitScale) / 2);
                this.updateTransform();
            }}

            toggleFullscreen() {{
                if (!document.fullscreenElement) {{
                    this.wrapper.requestFullscreen().then(() => {{
                        setTimeout(() => this.fitView(), 150);
                    }}).catch(err => {{
                        console.warn("Fullscreen error:", err);
                    }});
                }} else {{
                    document.exitFullscreen().then(() => {{
                        setTimeout(() => this.fitView(), 150);
                    }});
                }}
            }}

            updateTransform() {{
                this.canvas.style.transform = `translate(${{this.x}}px, ${{this.y}}px) scale(${{this.scale}})`;
                const percent = Math.round(this.scale * 100);
                if (this.badge) this.badge.textContent = `${{percent}}%`;
                if (this.slider) this.slider.value = percent;
            }}
        }}

        const viewers = {{}};

        document.addEventListener('DOMContentLoaded', () => {{
            viewers['tab-stack'] = new InteractiveSvgViewer('viewer-stack');
            viewers['tab-pipeline'] = new InteractiveSvgViewer('viewer-pipeline');
            viewers['tab-sequence'] = new InteractiveSvgViewer('viewer-sequence');
            viewers['tab-boundary'] = new InteractiveSvgViewer('viewer-boundary');
        }});

        function switchTab(tabId) {{
            document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
            document.querySelectorAll('.tab-panel').forEach(panel => panel.classList.remove('active'));

            const activeBtn = Array.from(document.querySelectorAll('.tab-btn')).find(b => b.getAttribute('onclick').includes(tabId));
            if (activeBtn) activeBtn.classList.add('active');

            const activePanel = document.getElementById(tabId);
            if (activePanel) {{
                activePanel.classList.add('active');
                if (viewers[tabId]) {{
                    setTimeout(() => {{
                        viewers[tabId].fitView();
                    }}, 40);
                }}
            }}
        }}
    </script>
</body>
</html>"""

out_path = "docs/00-architecture-overview/system_architecture_visualizer.html"
with open(out_path, "w", encoding="utf-8") as f:
    f.write(html_template)

print(f"Created {out_path} successfully! File size: {len(html_template)}")
