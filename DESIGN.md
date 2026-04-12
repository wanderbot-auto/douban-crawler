# Frontend Style Prompt

将本文件视为本项目的前端生成提示词。为 `sap-assistant/web-monitor` 设计或生成新页面、组件、区块时，默认严格遵守以下规则。

## 1. 产品气质

- 这是一个 **SOC / 运维 / 监控后台**，不是营销官网。
- 整体风格要 **专业、稳定、克制、清晰**。
- 视觉重点放在 **信息层级、状态表达、可操作性**，不要放在装饰。
- 默认使用 **浅色主题**，不要改成深色主站。
- 页面应该让人感觉“可靠、整洁、适合长时间盯盘”。

## 2. 总体视觉规则

- 页面背景固定使用 `bg-slate-50`。
- 内容承载层固定使用 **白色卡片**：`bg-white rounded-lg border border-slate-200 shadow-sm`。
- 主交互色固定为 **蓝色**：
  - 主色：`blue-500`
  - hover：`blue-600`
  - 强调/深色：`blue-700`
- 文本主要使用 `slate` 灰阶，不要随意引入彩色正文。
- 深色背景只允许出现在 **日志、终端输出、原始数据块** 等局部模块。

## 3. 颜色语义

### 中性色

- 页面背景：`bg-slate-50`
- 弱背景：`bg-slate-100`
- 标准边框：`border-slate-200`
- 输入边框：`border-slate-300`
- 主标题：`text-slate-800` 或 `text-slate-900`
- 正文：`text-slate-600`
- 辅助说明：`text-slate-500`
- 元数据/弱提示：`text-slate-400`

### 状态色

- 成功 / 正常运行：`green` 或 `emerald`
- 警告 / 等待 / 暂停 / 重试：`amber`
- 失败 / 高危 / 危险操作：`red` 或 `rose`
- 信息 / 处理中 / 当前激活：`blue` 或 `sky`

严格禁止：
- 不要把红色用于普通按钮。
- 不要把绿色用于主 CTA。
- 不要把状态色当成装饰色到处铺。

## 4. 字体与排版

- 使用系统字体栈：

```css
-apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Hiragino Sans GB',
'Microsoft YaHei', 'Helvetica Neue', Helvetica, Arial, sans-serif
```

- 默认正文：`text-sm`
- 页面标题：`text-2xl font-semibold`
- 卡片标题：`text-base font-medium` 或 `text-lg font-medium`
- 描述文案：`text-sm text-slate-500`
- 元信息：`text-xs text-slate-500` 或 `text-xs text-slate-400`
- 技术字段、ID、日志、时间：`font-mono`
- 重要数字/KPI：`text-2xl font-semibold`

不要：
- 不要使用花哨字体。
- 不要出现过多超大字号。
- 不要使用高饱和正文色。

## 5. 布局规则

- 主内容区默认 `p-6`。
- 页面主节奏默认 `space-y-6`。
- 常用间距：
  - `gap-2`：按钮组、小控件
  - `gap-3`：轻工具栏
  - `gap-4`：信息块
  - `gap-6`：页面主要区块
- 卡片内边距优先：
  - 标准：`p-6`
  - 紧凑：`p-4`
  - 小块：`p-3` / `p-2`
- 响应式优先纵向堆叠，不要为保列数牺牲可读性。
- 表格容器允许 `overflow-x-auto`。

## 6. 页面框架模板

所有新页面优先接近这个结构：

```vue
<template>
  <div class="space-y-6">
    <div class="flex items-center justify-between">
      <div>
        <h1 class="text-2xl font-semibold text-slate-800">页面标题</h1>
        <p class="mt-1 text-sm text-slate-500">一句话说明页面用途</p>
      </div>
      <div class="flex items-center gap-2">
        <button class="px-4 py-2 text-sm rounded-lg border border-slate-300 bg-white text-slate-700 hover:bg-slate-50 transition-colors">
          次操作
        </button>
        <button class="px-4 py-2 text-sm rounded-lg bg-blue-500 text-white hover:bg-blue-600 transition-colors">
          主操作
        </button>
      </div>
    </div>

    <div class="bg-white rounded-lg border border-slate-200 shadow-sm p-6">
      <!-- content -->
    </div>
  </div>
</template>
```

## 7. 组件生成规则

### 卡片

默认卡片：

```txt
bg-white rounded-lg border border-slate-200 shadow-sm p-6
```

可选增强：
- 强调卡片：`rounded-xl`
- hover：`hover:shadow-md transition-all duration-200`
- 内层浅区：`bg-slate-50 rounded-lg border border-slate-100`

### 按钮

主按钮：

```txt
px-4 py-2 text-sm rounded-lg bg-blue-500 text-white hover:bg-blue-600 transition-colors
```

次按钮：

```txt
px-4 py-2 text-sm rounded-lg border border-slate-300 bg-white text-slate-700 hover:bg-slate-50 transition-colors
```

危险按钮：

```txt
px-4 py-2 text-sm rounded-lg bg-red-500 text-white hover:bg-red-600 transition-colors
```

标签切换按钮组：

```txt
flex bg-slate-100 rounded-lg p-1
```

激活子按钮：

```txt
px-3 py-1 rounded-md bg-white text-blue-600 shadow-sm font-medium
```

未激活子按钮：

```txt
px-3 py-1 rounded-md text-slate-600 hover:text-slate-800
```

### 输入框

标准输入框：

```txt
px-3 py-2 text-sm border border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500
```

标准下拉框：

```txt
px-3 py-2 text-sm border border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white
```

### 表格

表格外层：

```txt
bg-white rounded-lg shadow-sm border border-slate-200 overflow-hidden
```

表头：

```txt
bg-slate-50 border-b border-slate-200
```

表头文字：

```txt
px-4 py-3 text-left text-xs font-medium text-slate-500 uppercase
```

行 hover：

```txt
hover:bg-slate-50 transition-colors
```

### 状态徽标

统一使用小胶囊：

```txt
text-xs px-2 py-1 rounded-full
```

状态颜色：
- success: `bg-green-100 text-green-800`
- info: `bg-blue-100 text-blue-800`
- warning: `bg-amber-100 text-amber-800`
- error: `bg-red-100 text-red-800`

### 日志 / 终端输出

仅对技术输出区使用深色块：

```txt
bg-slate-900 border border-slate-800 rounded-lg p-3 font-mono text-xs text-slate-300 shadow-inner
```

提示符/高亮：

```txt
text-emerald-400
```

### 弹窗

遮罩：

```txt
fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50
```

弹窗内容：

```txt
bg-white rounded-lg shadow-xl
```

## 8. 图表规则

- 图表永远放在白色卡片里。
- 首选主色 `#3b82f6`。
- 辅助颜色最多再加 2 到 3 个状态色。
- 图表背景保持干净，不要做重渐变。
- 监控图优先突出趋势、对比、异常，不要做炫技视觉。

## 9. 动效规则

- 动效必须轻量、短促、服务交互反馈。
- 推荐：
  - `transition-colors`
  - `transition-all duration-200`
  - `animate-spin`
  - `animate-pulse`
- 页面切换可用淡入淡出。
- 禁止复杂弹簧、悬浮漂移、大面积连续动画。

## 10. 生成时的硬性约束

当你为这个项目生成 UI 时，必须遵守：

- 不要生成营销官网风格。
- 不要生成玻璃拟态、霓虹、赛博紫、渐变大背景。
- 不要大面积深色化。
- 不要引入过多装饰图形。
- 不要混入多套按钮风格。
- 不要使用夸张圆角和重阴影。
- 不要让单页出现超过 1 个主强调色。
- 默认优先使用 Tailwind 工具类，不要先写大段自定义 CSS。

## 11. 一段可直接给 AI/Copilot 的简版提示词

```txt
请按照 sap-assistant/web-monitor 的现有后台风格生成界面：
这是一个 SOC / 运维 / 监控后台，不是营销官网。整体使用浅色主题，页面背景固定为 bg-slate-50，内容主要承载在白色卡片中，卡片样式统一为 bg-white rounded-lg border border-slate-200 shadow-sm。主交互色只用蓝色，主按钮使用 blue-500/600，文字系统使用 slate 灰阶，标题 text-slate-800/900，正文 text-slate-600，说明 text-slate-500，弱提示 text-slate-400。状态色严格语义化：成功 green/emerald，警告 amber，失败 red/rose，信息 blue/sky。布局保持 p-6 + space-y-6 的后台节奏，表格、筛选器、统计卡片、日志块是核心组件。输入框统一使用 rounded-lg + border-slate-300 + focus:ring-blue-500。表格统一使用白底、浅灰表头、hover:bg-slate-50。日志或终端输出区可以使用 bg-slate-900 + font-mono，但深色只限局部技术模块。整体风格务必克制、专业、可靠、高信息密度，避免渐变、玻璃拟态、霓虹色、夸张阴影和营销化设计。
```

## 12. 一句话总结

**像专业监控控制台，而不是像产品宣传页。**
