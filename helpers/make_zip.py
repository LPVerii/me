import os
import zipfile
    
def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".py") or file.endswith(".ipynb"):
                try:
                    ziph.write(os.path.join(root, file), 
                               os.path.relpath(os.path.join(root, file), 
                                               os.path.join(path, '..')))
                except PermissionError:
                    print(f"problem with {file}")
            else:
                print(f"skip {file}")
                
                
with zipfile.ZipFile('backup.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
    zipdir('.', zipf)