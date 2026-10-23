import { defineConfig } from 'vitepress'

function sidebar(prefix: string, en: boolean) {
  const pages = (items: string[][]) => items.map(([zh, english, path]) => ({
    text: en ? english : zh, link: `${prefix}${path}`,
  }))
  return [
    { text: en ? 'Getting started' : '入门', items: pages([
      ['介绍', 'Introduction', 'guide/'],
      ['安装与版本', 'Installation and versions', 'guide/installation'],
      ['首次运行', 'First run', 'guide/quickstart'],
      ['进程退出与恢复', 'Process exit and recovery', 'guide/recovery'],
    ]) },
    { text: en ? 'Usage guides' : '使用指南', items: pages([
      ['子图与重试恢复', 'Subgraphs and retries', 'guide/subgraphs'],
      ['编码业务值', 'Encoding business values', 'guide/codecs'],
      ['存储与运行所有权', 'Storage and ownership', 'guide/storage'],
      ['SQLite 使用教程', 'SQLite tutorial', 'guide/sqlite'],
      ['PostgreSQL 使用教程', 'PostgreSQL tutorial', 'guide/postgresql'],
      ['审批与工具组合', 'Approval and tool composition', 'guide/composition'],
      ['观测与运行身份', 'Observation and run identity', 'guide/observation'],
      ['检查点成本', 'Checkpoint costs', 'guide/checkpoint-cost'],
      ['统一生产框架对照', 'Unified workflow comparison', 'guide/production-comparison'],
      ['历史规模与性能', 'Historical scale and performance', 'guide/performance'],
    ]) },
    { text: en ? 'Reference' : '参考', collapsed: false, items: pages([
      ['参考索引', 'Reference index', 'reference/'],
      ['执行与恢复规则', 'Execution and recovery', 'reference/execution'],
      ['检查点记录模型', 'Checkpoint record model', 'reference/data-model'],
      ['错误与排查', 'Errors and troubleshooting', 'reference/errors'],
    ]) },
    { text: en ? 'Project' : '项目', collapsed: true, items: pages([
      ['品牌素材', 'Brand assets', 'reference/brand'],
    ]) },
  ]
}

function locale(en: boolean) {
  const prefix = en ? '/en/' : '/'
  const groups = sidebar(prefix, en)
  return {
    label: en ? 'English' : '简体中文', lang: en ? 'en-US' : 'zh-CN',
    ...(en ? { link: '/en/' } : {}),
    themeConfig: {
      nav: [
        { text: en ? 'Get started' : '入门', link: `${prefix}guide/`, activeMatch: `${prefix}guide/(?:installation|quickstart|recovery)?(?:\\.html)?$` },
        { text: en ? 'Guides' : '使用指南', link: `${prefix}guide/subgraphs`, activeMatch: `${prefix}guide/(?:subgraphs|codecs|storage|sqlite|postgresql|composition|observation|checkpoint-cost|performance)` },
        { text: en ? 'Reference' : '参考', link: `${prefix}reference/`, activeMatch: `${prefix}reference/` },
        { text: 'Pulse', link: en ? 'https://luo-root.github.io/pulse/en/' : 'https://luo-root.github.io/pulse/' },
      ],
      sidebar: { [`${prefix}guide/`]: groups, [`${prefix}reference/`]: groups },
      outline: { level: [2, 3] as [number, number], label: en ? 'On this page' : '本页目录' },
      docFooter: { prev: en ? 'Previous' : '上一篇', next: en ? 'Next' : '下一篇' },
      editLink: { pattern: 'https://github.com/Luo-root/pulse-durable/edit/main/site/:path', text: en ? 'Edit this page on GitHub' : '在 GitHub 编辑此页' },
      footer: { message: en ? 'Target v0.1.0 · Go 1.26+ · Pulse v0.5.0 · Release status: GitHub Releases · MIT' : '目标 v0.1.0 · Go 1.26+ · Pulse v0.5.0 · 发布状态见 GitHub Releases · MIT', copyright: 'Copyright © 2026 Luo-root' },
      ...(en ? {} : {
        darkModeSwitchLabel: '外观', lightModeSwitchTitle: '切换到浅色主题',
        darkModeSwitchTitle: '切换到深色主题', sidebarMenuLabel: '目录',
        returnToTopLabel: '返回顶部', langMenuLabel: '切换语言',
      }),
    },
  }
}

export default defineConfig({
  base: '/pulse-durable/',
  lang: 'zh-CN', title: 'Pulse Durable',
  description: 'Pulse 工作流的检查点执行与恢复。',
  head: [['link', { rel: 'icon', type: 'image/svg+xml', href: '/pulse-durable/favicon.svg' }]],
  locales: { root: locale(false), en: locale(true) },
  themeConfig: {
    logo: { light: '/brand/logo-light.svg', dark: '/brand/logo-dark.svg', alt: 'Pulse Durable' },
    siteTitle: false,
    socialLinks: [{ icon: 'github', link: 'https://github.com/Luo-root/pulse-durable' }],
    search: {
      provider: 'local', options: { locales: { root: { translations: {
        button: { buttonText: '搜索文档', buttonAriaLabel: '搜索文档' },
        modal: { noResultsText: '没有结果', resetButtonTitle: '清除查询',
          footer: { selectText: '选择', navigateText: '切换', closeText: '关闭' } },
      } } } },
    },
  },
})
