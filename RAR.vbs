Dim Fso
Dim Directory
Dim Modified
Dim Files
Dim MyDate
Dim OutputFile
Dim CommandToRun
MyDate = Replace(Date, "-", "/")



Set fs = CreateObject("Scripting.FileSystemObject")
Set MainFolder = fs.GetFolder("C:\YOUR\GDB\PATH")
For Each fldr In MainFolder.SubFolders
    ''As per comment
    If fldr.DateLastModified > LastDate Or IsEmpty(LastDate) Then
        LastFolder = fldr.Name
        LastDate = fldr.DateLastModified
    End If
Next

OutputFile = "RAR_NAME_" & mydate & ".rar" 
Set objShell = WScript.CreateObject("WScript.Shell")

objShell.Run "C:\windows\rar.exe a -ep1 C:\YOUR\GDB\PATH\" & OutputFile & " C:\YOUR\GDB\PATH\"  & LastFolder
