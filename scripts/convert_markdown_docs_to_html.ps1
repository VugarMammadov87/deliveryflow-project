$ErrorActionPreference = "Stop"

$root = (Resolve-Path ".").Path
$docsRoot = Join-Path $root "docs"
$assetCss = "assets/css/documentation.css"
$assetJs = "assets/js/documentation.js"

function Assert-InWorkspace {
    param([string] $Path)
    $resolved = (Resolve-Path $Path).Path
    if (-not $resolved.StartsWith($root, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to modify path outside workspace: $resolved"
    }
    return $resolved
}

function Encode {
    param([AllowNull()][string] $Value)
    return [System.Net.WebUtility]::HtmlEncode($Value)
}

function Slug {
    param([string] $Value)
    $slug = $Value.ToLowerInvariant() -replace '[^a-z0-9]+', '-'
    $slug = $slug.Trim('-')
    if ([string]::IsNullOrWhiteSpace($slug)) {
        return "section"
    }
    return $slug
}

function Convert-Inline {
    param([string] $Text)
    $encoded = Encode $Text
    $encoded = [regex]::Replace($encoded, '`([^`]+)`', '<code>$1</code>')
    $encoded = [regex]::Replace($encoded, '\*\*([^*]+)\*\*', '<strong>$1</strong>')
    $encoded = [regex]::Replace($encoded, '\[([^\]]+)\]\(([^)]+)\)', '<a href="$2">$1</a>')
    return $encoded
}

function Close-List {
    param([ref] $InList)
    if ($InList.Value) {
        $script:html.Add("</ul>")
        $InList.Value = $false
    }
}

function Convert-MarkdownBody {
    param([string[]] $Lines)

    $script:html = [System.Collections.Generic.List[string]]::new()
    $inCode = $false
    $inList = $false
    $paragraph = [System.Collections.Generic.List[string]]::new()
    $headings = [System.Collections.Generic.List[object]]::new()

    function Flush-Paragraph {
        if ($paragraph.Count -gt 0) {
            $text = ($paragraph -join " ").Trim()
            if ($text.Length -gt 0) {
                $script:html.Add("<p>$(Convert-Inline $text)</p>")
            }
            $paragraph.Clear()
        }
    }

    foreach ($line in $Lines) {
        if ($line -match '^\s*```') {
            Flush-Paragraph
            Close-List ([ref]$inList)
            if (-not $inCode) {
                $inCode = $true
                $script:html.Add("<pre><code>")
            } else {
                $inCode = $false
                $script:html.Add("</code></pre>")
            }
            continue
        }

        if ($inCode) {
            $script:html.Add((Encode $line))
            continue
        }

        if ([string]::IsNullOrWhiteSpace($line)) {
            Flush-Paragraph
            Close-List ([ref]$inList)
            continue
        }

        if ($line -match '^(#{1,6})\s+(.+)$') {
            Flush-Paragraph
            Close-List ([ref]$inList)
            $level = [Math]::Min($matches[1].Length + 1, 6)
            $title = $matches[2].Trim()
            $id = Slug $title
            $headings.Add([pscustomobject]@{ Title = $title; Id = $id; Level = $level }) | Out-Null
            $script:html.Add("<h$level id=""$id"">$(Convert-Inline $title)</h$level>")
            continue
        }

        if ($line -match '^\s*[-*]\s+(.+)$') {
            Flush-Paragraph
            if (-not $inList) {
                $script:html.Add("<ul>")
                $inList = $true
            }
            $script:html.Add("<li>$(Convert-Inline $matches[1].Trim())</li>")
            continue
        }

        if ($line -match '^\s*\d+\.\s+(.+)$') {
            Flush-Paragraph
            if (-not $inList) {
                $script:html.Add("<ul>")
                $inList = $true
            }
            $script:html.Add("<li>$(Convert-Inline $matches[1].Trim())</li>")
            continue
        }

        if ($line -match '^\s*>\s+(.+)$') {
            Flush-Paragraph
            Close-List ([ref]$inList)
            $script:html.Add("<blockquote>$(Convert-Inline $matches[1].Trim())</blockquote>")
            continue
        }

        $paragraph.Add($line.Trim())
    }

    Flush-Paragraph
    Close-List ([ref]$inList)

    return [pscustomobject]@{
        Body = ($script:html -join [Environment]::NewLine)
        Headings = $headings
    }
}

function Page-Chrome {
    param(
        [string] $Title,
        [string] $Subtitle,
        [string] $Body,
        [string] $CssPath,
        [string] $JsPath,
        [object[]] $Headings = @()
    )

    $toc = ""
    if ($Headings.Count -gt 0) {
        $items = foreach ($heading in $Headings | Select-Object -First 24) {
            "<a href=""#$($heading.Id)"">$([System.Net.WebUtility]::HtmlEncode($heading.Title))</a>"
        }
        $toc = "<aside class=""doc-toc"" aria-label=""Page sections"">$($items -join [Environment]::NewLine)</aside>"
    }

    return @"
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>$([System.Net.WebUtility]::HtmlEncode($Title)) | DeliveryFlow Docs</title>
    <link rel="stylesheet" href="$CssPath">
    <script src="$JsPath" defer></script>
  </head>
  <body>
    <a class="skip-link" href="#main">Skip to content</a>
    <header class="site-shell">
      <nav class="portal-nav" aria-label="Documentation navigation">
        <a class="brand" href="index.html">DeliveryFlow Docs</a>
        <div>
          <a href="architecture-decisions.html">Architecture</a>
          <a href="application-workflow.html">Workflow</a>
          <a href="superset-serving.html">BI</a>
          <a href="scalability-multi-application.html">Scale</a>
        </div>
        <button class="theme-toggle" type="button" data-theme-toggle aria-label="Toggle color theme">Theme</button>
      </nav>
      <section class="doc-hero">
        <p class="eyebrow">Converted Documentation</p>
        <h1>$([System.Net.WebUtility]::HtmlEncode($Title))</h1>
        <p>$([System.Net.WebUtility]::HtmlEncode($Subtitle))</p>
      </section>
    </header>
    <main id="main" class="doc-layout">
      $toc
      <article class="doc-article">
        $Body
      </article>
    </main>
  </body>
</html>
"@
}

$markdownFiles = @(
    "README.md",
    "requirement.md",
    "plan.md",
    "docs/application-workflow.md",
    "docs/architecture-decisions.md",
    "docs/postgres-source-tables.md",
    "docs/superset-serving.md",
    "docs/scalability-multi-application.md"
)

$convertedPages = [System.Collections.Generic.List[object]]::new()

foreach ($relativePath in $markdownFiles) {
    $mdPath = Assert-InWorkspace (Join-Path $root $relativePath)
    if (-not (Test-Path $mdPath)) {
        continue
    }

    $lines = Get-Content $mdPath
    $firstHeading = ($lines | Where-Object { $_ -match '^#\s+' } | Select-Object -First 1)
    if ($firstHeading) {
        $title = ($firstHeading -replace '^#\s+', '').Trim()
    } else {
        $title = [System.IO.Path]::GetFileNameWithoutExtension($relativePath)
    }

    $converted = Convert-MarkdownBody $lines
    $directory = [System.IO.Path]::GetDirectoryName($relativePath)
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($relativePath)
    $htmlRelative = if ([string]::IsNullOrWhiteSpace($directory)) { "$baseName.html" } else { "$directory/$baseName.html" }
    $htmlPath = Join-Path $root $htmlRelative

    $cssPath = if ([string]::IsNullOrWhiteSpace($directory)) { "docs/$assetCss" } else { $assetCss }
    $jsPath = if ([string]::IsNullOrWhiteSpace($directory)) { "docs/$assetJs" } else { $assetJs }
    $subtitle = "Converted from $relativePath. The Markdown source remains available for editing and review."
    $page = Page-Chrome -Title $title -Subtitle $subtitle -Body $converted.Body -CssPath $cssPath -JsPath $jsPath -Headings $converted.Headings
    Set-Content -Path $htmlPath -Value $page -Encoding utf8

    $convertedPages.Add([pscustomobject]@{
        Title = $title
        Md = $relativePath
        Html = $htmlRelative
        RootLink = $htmlRelative
        DocsLink = if ([string]::IsNullOrWhiteSpace($directory)) { "../$htmlRelative" } else { "$baseName.html" }
    }) | Out-Null
}

$indexPath = Join-Path $docsRoot "index.html"
$portalCards = foreach ($page in $convertedPages) {
    $href = $page.DocsLink
    "<a class=""portal-card"" href=""$href""><span>Converted</span><strong>$([System.Net.WebUtility]::HtmlEncode($page.Title))</strong><small>$([System.Net.WebUtility]::HtmlEncode($page.Md))</small></a>"
}

$index = @"
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>DeliveryFlow Documentation Portal</title>
    <meta name="description" content="Modern static HTML documentation portal for DeliveryFlow.">
    <link rel="stylesheet" href="assets/css/documentation.css">
    <script src="assets/js/documentation.js" defer></script>
  </head>
  <body>
    <a class="skip-link" href="#main">Skip to content</a>
    <header class="portal-hero">
      <nav class="portal-nav" aria-label="Documentation navigation">
        <a class="brand" href="index.html">DeliveryFlow Docs</a>
        <div>
          <a href="architecture-decisions.html">Architecture</a>
          <a href="application-workflow.html">Workflow</a>
          <a href="superset-serving.html">BI</a>
          <a href="scalability-multi-application.html">Scale</a>
        </div>
        <button class="theme-toggle" type="button" data-theme-toggle aria-label="Toggle color theme">Theme</button>
      </nav>
      <section class="portal-hero__content">
        <p class="eyebrow">Static Engineering Portal</p>
        <h1>DeliveryFlow</h1>
        <p>
          A local data platform guide for streaming, batch, serving, BI, and
          multi-application growth. Delivery is the default application; Fleet is
          selected with <code>APP=fleet</code>.
        </p>
        <div class="metric-strip" aria-label="Documentation highlights">
          <div><strong>8</strong><span>HTML documents</span></div>
          <div><strong>2</strong><span>applications</span></div>
          <div><strong>10</strong><span>platform services</span></div>
          <div><strong>0</strong><span>external UI deps</span></div>
        </div>
      </section>
    </header>

    <main id="main">
      <section class="section">
        <div class="section-heading">
          <p class="eyebrow">System Flow</p>
          <h2>Operational paths rendered as HTML diagrams</h2>
        </div>
        <div class="diagram-board">
          <article>
            <h3>Delivery Stream and BI</h3>
            <div class="pipeline">
              <span>Producer</span><b></b><span>Kafka<br>delivery-events</span><b></b><span>Flink</span><b></b><span>ClickHouse<br>delivery.*</span><b></b><span>Superset</span>
            </div>
          </article>
          <article>
            <h3>Fleet Telemetry</h3>
            <div class="pipeline">
              <span>APP=fleet</span><b></b><span>Kafka<br>vehicle-telemetry-events</span><b></b><span>Flink SQL</span><b></b><span>ClickHouse<br>fleet.*</span>
            </div>
          </article>
          <article>
            <h3>Central Configuration</h3>
            <div class="pipeline">
              <span>.env</span><b></b><span>configs/platform.yaml</span><b></b><span>Python/Java readers</span><b></b><span>Apps and scripts</span>
            </div>
          </article>
        </div>
      </section>

      <section class="section section--band">
        <div class="section-heading">
          <p class="eyebrow">Document Library</p>
          <h2>All Markdown documentation converted to HTML</h2>
        </div>
        <div class="portal-grid">
          $($portalCards -join [Environment]::NewLine)
        </div>
      </section>
    </main>
  </body>
</html>
"@
Set-Content -Path $indexPath -Value $index -Encoding utf8

Write-Output "Converted $($convertedPages.Count) Markdown files to HTML. Markdown sources were preserved."
foreach ($page in $convertedPages) {
    Write-Output "$($page.Md) -> $($page.Html)"
}
