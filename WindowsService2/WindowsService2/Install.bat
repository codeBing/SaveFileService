%SystemRoot%\Microsoft.NET\Framework\v4.0.30319\installutil.exe WindowsService2.exe
Net Start SaveService
sc config ServiceTest start= auto