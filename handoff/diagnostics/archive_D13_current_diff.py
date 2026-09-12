"""Archive raw patch bytes without treating patch context as Markdown whitespace."""
import pathlib,subprocess,zipfile
diff=subprocess.check_output(['git','-C',r'C:\wsl\fcp-fix-d13-windows-refusal-response-20260912','diff'])
with zipfile.ZipFile(pathlib.Path(__file__).resolve().parent/'D13-development-repair.zip','w',zipfile.ZIP_DEFLATED) as archive:
    archive.writestr('D13-development-repair.patch',diff)
