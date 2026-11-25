param(
    [string]$DocumentRelativePath = 'output\docx\Bao_cao_DATN_Dinh_Cong_Minh_2026-11-07.docx',
    [string]$PdfRelativePath = 'tmp\remediation-20261107\thesis-revision.pdf'
)
$ErrorActionPreference = 'Stop'
$taskRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$taskDocPath = Join-Path $taskRoot $DocumentRelativePath
$taskPdfPath = Join-Path $taskRoot $PdfRelativePath
$taskWord = $null
$taskDocument = $null
try {
    $taskWord = New-Object -ComObject Word.Application
    $taskWord.Visible = $false
    $taskWord.DisplayAlerts = 0
    $taskDocument = $taskWord.Documents.Open($taskDocPath, $false, $false)
    $taskDocument.Repaginate()
    $taskDocument.Fields.Update() | Out-Null
    foreach ($taskToc in $taskDocument.TablesOfContents) { $taskToc.Update() }
    foreach ($taskTof in $taskDocument.TablesOfFigures) {
        $taskTof.Update()
        $taskTof.Range.ParagraphFormat.TabStops.ClearAll()
        $taskTab = $taskTof.Range.ParagraphFormat.TabStops.Add($taskWord.CentimetersToPoints(15), 2, 1)
        $taskTof.Range.ParagraphFormat.LeftIndent = 0
        $taskTof.Range.ParagraphFormat.RightIndent = 18
        $taskTof.Range.ParagraphFormat.FirstLineIndent = 0
        $taskTof.Range.Font.Size = 12
        $taskTof.Range.ParagraphFormat.LineSpacingRule = 0
        $taskTof.Range.ParagraphFormat.SpaceBefore = 0
        $taskTof.Range.ParagraphFormat.SpaceAfter = 5
    }
    $taskDocument.Repaginate()
    $taskDocument.Save()
    $taskDocument.ExportAsFixedFormat($taskPdfPath, 17)
    Write-Output ('Rendered pages: ' + $taskDocument.ComputeStatistics(2))
} finally {
    if ($null -ne $taskDocument) { try { $taskDocument.Close(0) } catch {} }
    if ($null -ne $taskWord) { try { $taskWord.Quit() } catch {} }
}
