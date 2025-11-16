uv python list-available

uv python install 3.12
uv python install 3.11
uv python install 3.10.14

uv python list

uv venv --python 3.12

# install as one app
uv run pip install pyinstaller

uv run pyinstaller --onefile main.py # produces one file
dist/main   # This is your standalone executable
./dist/main

Step 4: Distribute
Just give the user the dist/main file.
They also need your .env file` in the same directory (or somewhere the program can read it).
2️⃣ Notes about .env and secrets
Your app still needs a .env file for DB host, user, etc.
Prompting for the password at runtime works fine — the executable will still prompt.

Ah, this is a very common PyInstaller gotcha — some packages, especially those with C extensions like psycopg2, are not automatically detected. PyInstaller builds a “dependency graph,” but binary packages or hidden imports can get missed. That’s why your standalone executable can’t find psycopg2.
Here’s how to fix it.
1️⃣ Use psycopg2-binary instead of psycopg2
On Linux, psycopg2-binary is self-contained and doesn’t require dynamic linking to libpq.
Install it in your .venv:
uv run pip uninstall psycopg2 -y
uv run pip install psycopg2-binary
This usually fixes most PyInstaller issues with Postgres.
2️⃣ Tell PyInstaller about hidden imports
Sometimes PyInstaller needs a hint:
uv run pyinstaller --onefile --hidden-import=psycopg2 main.py
--hidden-import=psycopg2 ensures PyInstaller bundles the C extension.
If you have other modules like boto3 or rich not detected, you can add multiple hidden imports:
uv run pyinstaller --onefile \
    --hidden-import=psycopg2 \
    --hidden-import=boto3 \
    --hidden-import=rich \
    main.py
3️⃣ Clean build and rebuild
It’s often easiest to delete build/ and dist/ before rebuilding:
rm -rf build dist
uv run pyinstaller --onefile --hidden-import=psycopg2 main.py
4️⃣ Check the executable
./dist/main


Clean the old build
rm -rf build dist main.spec
2️⃣ Rebuild with PyInstaller and hidden imports
uv run pyinstaller --onefile \
    --hidden-import=psycopg2 \
    --hidden-import=boto3 \
    --hidden-import=rich \
    main.py
--onefile → single executable.
--hidden-import → ensures PyInstaller includes modules that sometimes aren’t detected automatically.
After this, the executable will be in:
dist/main
3️⃣ Test the new executable
./dist/main


