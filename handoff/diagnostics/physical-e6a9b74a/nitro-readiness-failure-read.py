import pathlib,json
root=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
print((root/'evidence/linux/commands.txt').read_text())
