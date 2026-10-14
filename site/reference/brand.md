# 品牌素材

Durable 的标志用于仓库、文档与项目介绍。图形沿用 Pulse 的蓝色与波形：开放的 D 形轮廓对应 Durable，波形表示继续执行，底部短线表示保存的检查点。它表达恢复层的定位，不表示外部副作用只会发生一次。

## 标准组合

浅色背景：

![Durable 浅色字标](/brand/logo-light.svg)

深色背景应使用 `logo-dark.svg`。README 使用蓝底组合 `logo.svg`，背景与字标对比度不依赖读者主题。

## 文件选择

| 文件 | 用途 |
| --- | --- |
| `logo.svg` / `logo.png` | 蓝底横版组合，适合 README 与展示 |
| `logo-light.svg` | 透明背景、深色字标，适合浅色页面 |
| `logo-dark.svg` | 透明背景、浅色字标，适合深色页面 |
| `mark.svg` / `mark.png` | 独立蓝色图形 |
| `mark-mono.svg` / `mark-inverse.svg` | 单色印刷与反白 |
| `favicon.svg` / `favicon.png` | 小尺寸图标；省略检查点短线以保持清晰 |

仓库源素材位于 [`.github/assets`](https://github.com/Luo-root/pulse-durable/tree/main/.github/assets)，站点使用 `site/public/brand/` 中的副本。SVG 字标已转换为路径，不依赖读者安装字体或远程字体服务。

## 使用规则

保持长宽比例，四周至少保留相当于图形线宽两倍的空白。标准独立图形建议不小于 32px；16px 到 24px 使用简化 favicon。横版组合建议宽度不小于 180px，尺寸更小时使用独立图标。

主色为 `#1E40AF`，深色背景的信号线为 `#60A5FA`。单色版本应整体改色，不单独移动轮廓、波形或检查点短线。不要添加阴影、改变线宽或拉伸图形。

字标基于 DejaVu Sans Bold 并已转为轮廓。字体版权说明随素材保存于 `FONT-LICENSE.txt`；项目素材按仓库许可证使用。

下一步：[返回参考索引](index.md)。
