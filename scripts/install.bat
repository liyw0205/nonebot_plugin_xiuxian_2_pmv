@echo off
mode con cols=80 lines=25
chcp 65001 >nul

setlocal EnableDelayedExpansion
set "LINE========================================="

title xiuxian
set "PORT=8080"
set "DEFAULT_DRIVE=C"
call :select_drive
mkdir "%DIR%" 2>nul

:zhuye
cls
color 3f
echo %LINE%
set "IP="
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /c:"IPv4"') do if not defined IP set "IP=%%a"
if defined IP (
    set "IP=!IP:~1!"
) else (
    set "IP=127.0.0.1"
)
echo WLAN/IP 地址: !IP!
echo 当前安装目录: %DIR%\xiu2
echo 项目地址：https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv
echo OneBot V11 协议地址：
echo     ws://127.0.0.1:%PORT%/onebot/v11/ws
echo %LINE%
echo A.启动  B.安装  C.重装  D.更新  E.更新依赖  F.切换安装盘
echo %LINE%

set choice=
set /p choice=请输入对应字母后回车:
cls
if /i "%choice%"=="A" (
    color 07
    cd /d "%DIR%"
    if not exist "%DIR%\myenv\Scripts\activate.bat" (
        echo 未找到虚拟环境，请先安装（B）。
        pause
        goto zhuye
    )
    call "%DIR%\myenv\Scripts\activate.bat"
    cd /d "%DIR%\xiu2"
    nb run --reload
    goto zhuye
)

if /i "%choice%"=="B" goto install
if /i "%choice%"=="C" goto uninstall
if /i "%choice%"=="D" goto update
if /i "%choice%"=="E" goto update_deps
if /i "%choice%"=="F" (
    call :select_drive
    mkdir "!DIR!" 2>nul
    goto zhuye
)
echo 输入错误，请重新选择！
echo %LINE%
echo 请按任意键继续...
pause > nul
goto zhuye

:install
goto check

:check
cls
echo %LINE%
echo 正在检测 Python 环境...
echo %LINE%

set "PYTHON_INSTALLED="
for /f "tokens=1,2" %%i in ('python -c "import sys; print(sys.version_info.major, sys.version_info.minor)" 2^>nul') do (
    set "PYTHON_INSTALLED=true"
)

if "%PYTHON_INSTALLED%"=="true" goto install_project

echo 未检测到 Python。将尝试安装 Python 3.11.0。
set "PYTHON_INSTALLER_URL=https://www.python.org/ftp/python/3.11.0/python-3.11.0-amd64.exe"
set "PYTHON_INSTALLER_PATH=%DIR%\python-3.11.0-amd64.exe"

if exist "%cd%\python-3.11.0-amd64.exe" (
    echo 当前路径存在 python-3.11.0-amd64.exe ... 已移动到 %DIR%\python-3.11.0-amd64.exe
    move "%cd%\python-3.11.0-amd64.exe" "%DIR%\python-3.11.0-amd64.exe" >nul
)

if not exist "%PYTHON_INSTALLER_PATH%" (
    echo 下载地址: %PYTHON_INSTALLER_URL%
    echo 正在下载 Python 3.11.0 安装包...
    call :download_file "%PYTHON_INSTALLER_URL%" "%PYTHON_INSTALLER_PATH%"
)

echo 正在安装 Python 3.11.0 (静默安装，请稍候)...
start /wait "" "%PYTHON_INSTALLER_PATH%" /quiet InstallAllUsers=1 PrependPath=1 Include_test=0

echo %LINE%
echo 请按任意键退出，重新打开脚本来加载环境...
pause > nul
start "" "%~dpnx0"
exit

:install_project
cls
echo %LINE%
echo          开始安装 Xiu2 项目
echo %LINE%

rmdir /s /q "%DIR%\tmp" 2>nul
mkdir "%DIR%\tmp" 2>nul

call :select_proxy
echo %LINE%

set "download_url=https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/releases/latest/download/project.tar.gz"

if exist "%cd%\project.tar.gz" (
    echo 当前路径存在 project.tar.gz ... 已移动到 %DIR%\project.tar.gz
    move "%cd%\project.tar.gz" "%DIR%\project.tar.gz" >nul
)

echo [1/7] 检测 project.tar.gz ...
if not exist "%DIR%\project.tar.gz" (
    echo [1/7] 正在下载 project.tar.gz ...
    echo 下载地址: !proxy!!download_url!
    call :download_file "!proxy!!download_url!" "%DIR%\project.tar.gz"
    if errorlevel 1 (
        echo 下载失败！请检查网络或代理。
        echo %LINE%
        pause > nul
        goto zhuye
    )
)

echo [2/7] 创建项目结构和 pyproject.toml ...
mkdir "%DIR%\xiu2\src\plugins" 2>nul
mkdir "%DIR%\xiu2\data" 2>nul

(
echo [project]
echo name = "xiu2"
echo version = "0.1.0"
echo description = "xiu2"
echo readme = "README.md"
echo requires-python = ">=3.9, ^<4.0"
echo dependencies = [
echo     "nonebot2[fastapi]>=2.4.4",
echo     "nonebot2[httpx]>=2.4.4",
echo     "nonebot2[websockets]>=2.4.4",
echo     "nonebot2[aiohttp]>=2.4.4",
echo     "nonebot-adapter-onebot>=2.4.6",
echo     "nonebot-adapter-qq>=1.7.1"
echo ]
echo.
echo [project.optional-dependencies]
echo dev = []
echo.
echo [tool.nonebot]
echo plugin_dirs = ["src/plugins"]
echo builtin_plugins = []
echo.
echo [tool.nonebot.adapters]
echo nonebot-adapter-onebot = [
echo     { name = "OneBot V11", module_name = "nonebot.adapters.onebot.v11" }
echo ]
echo "@local" = []
echo nonebot-adapter-qq = [{name = "QQ", module_name = "nonebot.adapters.qq"}]
echo.
echo [tool.nonebot.plugins]
echo "@local" = []
) > "%DIR%\xiu2\pyproject.toml"

echo [3/7] 解压 project.tar.gz ...
python -c "import tarfile; tf = tarfile.open(r'%DIR%\project.tar.gz', 'r:gz'); tf.extractall(r'%DIR%\tmp'); tf.close(); print('解压完成')" || (
    echo Python 解压失败！
    pause > nul
    goto zhuye
)

move "%DIR%\tmp\data\xiuxian" "%DIR%\xiu2\data" >nul
move "%DIR%\tmp\nonebot_plugin_xiuxian_2" "%DIR%\xiu2\src\plugins" >nul
if exist "%DIR%\tmp\requirements.txt" move /y "%DIR%\tmp\requirements.txt" "%DIR%\xiu2\requirements.txt" >nul

echo [4/7] 创建虚拟环境 ...
python -m venv "%DIR%\myenv"

echo [5/7] 安装依赖（使用清华镜像）...
cd /d "%DIR%"
call "%DIR%\myenv\Scripts\activate.bat"
cd /d "%DIR%\xiu2"
call :update_python_dependencies
if errorlevel 1 (
    echo 依赖安装失败！
    pause >nul
    goto zhuye
)

echo [6/7] 创建配置文件 ...
(
echo ENVIRONMENT=dev
echo DRIVER=~fastapi+~httpx+~websockets+~aiohttp
) > "%DIR%\xiu2\.env"

(
echo LOG_LEVEL=INFO
echo.
echo SUPERUSERS = ["123456"]
echo COMMAND_START = [""]
echo NICKNAME = ["堂堂"]
echo DEBUG = False
echo HOST = 0.0.0.0
echo PORT = %PORT%
) > "%DIR%\xiu2\.env.dev"

echo [7/7] 获取本机IP并显示启动信息 ...
set "IPV4="
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /c:"IPv4"') do if not defined IPV4 set "IPV4=%%a"
if defined IPV4 (
    set "IPV4=!IPV4:~1!"
) else (
    set "IPV4=127.0.0.1"
)

(
echo color 07
echo cd /d "%DIR%"
echo call "%DIR%\myenv\Scripts\activate.bat"
echo cd /d "%DIR%\xiu2"
echo nb run --reload
) > "%DIR%\启动修仙.bat"

echo.
echo %LINE%
echo 安装完成！
echo 安装目录: %DIR%\xiu2
echo 数据库: SQLite（本地 data\xiuxian\*.db）
echo ws://!IPV4!:%PORT%/onebot/v11/ws
echo ws://127.0.0.1:%PORT%/onebot/v11/ws
echo %LINE%

rmdir /s /q "%DIR%\tmp" 2>nul
call "%DIR%\启动修仙.bat"
echo 已尝试启动修仙
echo 请按任意键继续...
pause > nul
goto zhuye

:update
cls
echo %LINE%
echo            开始更新 Xiu2 项目
echo %LINE%

if not exist "%DIR%\xiu2" (
    echo 未检测到项目目录，将自动进入安装流程...
    timeout /t 2 >nul
    goto install
)

rmdir /s /q "%DIR%\tmp" 2>nul
mkdir "%DIR%\tmp" 2>nul

call :select_proxy
echo %LINE%

set "download_url=https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/releases/latest/download/project.tar.gz"

if exist "%DIR%\project.tar.gz" del /f /q "%DIR%\project.tar.gz" >nul 2>nul

echo [1/5] 下载最新 project.tar.gz ...
echo 下载地址: !proxy!!download_url!
call :download_file "!proxy!!download_url!" "%DIR%\project.tar.gz"
if errorlevel 1 (
    echo 下载失败！请检查网络或代理。
    pause > nul
    goto zhuye
)

echo [2/5] 解压更新包 ...
python -c "import tarfile; tf = tarfile.open(r'%DIR%\project.tar.gz', 'r:gz'); tf.extractall(r'%DIR%\tmp'); tf.close(); print('解压完成')" || (
    echo Python 解压失败！
    pause > nul
    goto zhuye
)

echo [3/5] 覆盖插件与数据 ...
if not exist "%DIR%\tmp\nonebot_plugin_xiuxian_2\__init__.py" (
    echo 更新包缺少 nonebot_plugin_xiuxian_2\__init__.py，已取消更新。
    pause > nul
    goto zhuye
)
if exist "%DIR%\xiu2\src\plugins\nonebot_plugin_xiuxian_2" rmdir /s /q "%DIR%\xiu2\src\plugins\nonebot_plugin_xiuxian_2"
mkdir "%DIR%\xiu2\src\plugins\nonebot_plugin_xiuxian_2" 2>nul
mkdir "%DIR%\xiu2\data" 2>nul
if exist "%DIR%\tmp\nonebot_plugin_xiuxian_2" (
    xcopy "%DIR%\tmp\nonebot_plugin_xiuxian_2\*" "%DIR%\xiu2\src\plugins\nonebot_plugin_xiuxian_2\" /E /I /Y >nul
    if errorlevel 1 (
        echo 插件文件覆盖失败！
        pause >nul
        goto zhuye
    )
    findstr /S /R /C:"from nonebot_plugin_xiuxian_2" /C:"import nonebot_plugin_xiuxian_2" "%DIR%\xiu2\src\plugins\nonebot_plugin_xiuxian_2\*.py" >nul
    if not errorlevel 1 (
        echo 部署代码仍包含不兼容 src.plugins 命名空间的绝对导入，已停止更新。
        pause >nul
        goto zhuye
    )
)
if exist "%DIR%\tmp\data" (
    xcopy "%DIR%\tmp\data\*" "%DIR%\xiu2\data\" /E /I /Y >nul
)
if exist "%DIR%\tmp\requirements.txt" move /y "%DIR%\tmp\requirements.txt" "%DIR%\xiu2\requirements.txt" >nul

echo [4/5] 更新依赖（按需）...
cd /d "%DIR%"
if exist "%DIR%\myenv\Scripts\activate.bat" (
    call "%DIR%\myenv\Scripts\activate.bat"
    cd /d "%DIR%\xiu2"
    call :update_python_dependencies
    if errorlevel 1 (
        echo 依赖更新失败！
        pause >nul
        goto zhuye
    )
)
if not exist "%DIR%\myenv\Scripts\activate.bat" (
    echo 未找到虚拟环境，已跳过依赖更新。
)

echo [5/5] 清理临时文件 ...
rmdir /s /q "%DIR%\tmp" 2>nul

echo.
echo %LINE%
echo 更新完成！
echo 如更新后无法启动，可尝试执行“C.重装”。
echo %LINE%
pause >nul
goto zhuye

:update_deps
cls
echo %LINE%
echo            开始更新 Python 依赖
echo %LINE%

cd /d "%DIR%"
if not exist "%DIR%\myenv\Scripts\activate.bat" (
    echo 未找到虚拟环境，请先安装（B）。
    pause >nul
    goto zhuye
)

call "%DIR%\myenv\Scripts\activate.bat"
cd /d "%DIR%\xiu2"
call :update_python_dependencies
if errorlevel 1 (
    echo 依赖更新失败！
    pause >nul
    goto zhuye
)

echo.
echo %LINE%
echo 依赖更新完成。
echo %LINE%
pause >nul
goto zhuye

:uninstall
echo %LINE%
echo A.确认重装（这会删除数据）  B.取消
echo %LINE%

set choice=
set /p choice=请输入对应字母后回车:
cls
if /i "%choice%"=="A" (
    rmdir /s /q "%DIR%\tmp" 2>nul
    rmdir /s /q "%DIR%\myenv" 2>nul
    rmdir /s /q "%DIR%\xiu2" 2>nul
    goto install
)
goto zhuye

:select_drive
cls
echo %LINE%
echo              请选择安装盘
echo %LINE%
echo 本机磁盘空间：
call :show_disk_space
echo %LINE%
echo 直接回车默认使用 %DEFAULT_DRIVE%: 盘，安装目录为 盘符:\nb
set "DRIVE_CHOICE="
set /p DRIVE_CHOICE=请输入安装盘符:
if not defined DRIVE_CHOICE set "DRIVE_CHOICE=%DEFAULT_DRIVE%"
set "DRIVE_CHOICE=%DRIVE_CHOICE:"=%"
set "DRIVE_CHOICE=%DRIVE_CHOICE::=%"
set "DRIVE_CHOICE=%DRIVE_CHOICE:\=%"
set "DRIVE_CHOICE=%DRIVE_CHOICE:/=%"
set "DRIVE_CHOICE=%DRIVE_CHOICE: =%"
set "DRIVE_CHOICE=%DRIVE_CHOICE:~0,1%"
if not defined DRIVE_CHOICE set "DRIVE_CHOICE=%DEFAULT_DRIVE%"
for %%D in (A B C D E F G H I J K L M N O P Q R S T U V W X Y Z) do if /i "%DRIVE_CHOICE%"=="%%D" set "DRIVE_CHOICE=%%D"
if not exist "%DRIVE_CHOICE%:\" (
    echo 未检测到 %DRIVE_CHOICE%: 盘，请重新选择。
    pause >nul
    goto select_drive
)
set "DIR=%DRIVE_CHOICE%:\nb"
echo 已选择安装目录: %DIR%
timeout /t 1 >nul
exit /b 0

:show_disk_space
set "PS_DISK_SPACE=JABFAHIAcgBvAHIAQQBjAHQAaQBvAG4AUAByAGUAZgBlAHIAZQBuAGMAZQAgAD0AIAAnAFMAdABvAHAAJwAKAEcAZQB0AC0AQwBpAG0ASQBuAHMAdABhAG4AYwBlACAAVwBpAG4AMwAyAF8ATABvAGcAaQBjAGEAbABEAGkAcwBrACAALQBGAGkAbAB0AGUAcgAgACcARAByAGkAdgBlAFQAeQBwAGUAPQAzACcAIAB8AAoAIAAgACAAIABTAG8AcgB0AC0ATwBiAGoAZQBjAHQAIABEAGUAdgBpAGMAZQBJAEQAIAB8AAoAIAAgACAAIABGAG8AcgBFAGEAYwBoAC0ATwBiAGoAZQBjAHQAIAB7AAoAIAAgACAAIAAgACAAIAAgACQAdABvAHQAYQBsACAAPQAgAFsAbQBhAHQAaABdADoAOgBSAG8AdQBuAGQAKAAkAF8ALgBTAGkAegBlACAALwAgADEARwBCACwAIAAyACkACgAgACAAIAAgACAAIAAgACAAJABmAHIAZQBlACAAPQAgAFsAbQBhAHQAaABdADoAOgBSAG8AdQBuAGQAKAAkAF8ALgBGAHIAZQBlAFMAcABhAGMAZQAgAC8AIAAxAEcAQgAsACAAMgApAAoAIAAgACAAIAAgACAAIAAgACQAdQBzAGUAZAAgAD0AIABbAG0AYQB0AGgAXQA6ADoAUgBvAHUAbgBkACgAKAAkAF8ALgBTAGkAegBlACAALQAgACQAXwAuAEYAcgBlAGUAUwBwAGEAYwBlACkAIAAvACAAMQBHAEIALAAgADIAKQAKACAAIAAgACAAIAAgACAAIAAnAHsAMAAsAC0ANAB9ACAAO2B6evSVOgAgAHsAMQAsADEAMAA6AE4AMgB9ACAARwBCACAAIADyXSh1OgAgAHsAMgAsADEAMAA6AE4AMgB9ACAARwBCACAAIADvUyh1OgAgAHsAMwAsADEAMAA6AE4AMgB9ACAARwBCACcAIAAtAGYAIAAkAF8ALgBEAGUAdgBpAGMAZQBJAEQALAAgACQAdABvAHQAYQBsACwAIAAkAHUAcwBlAGQALAAgACQAZgByAGUAZQAKACAAIAAgACAAfQA="
powershell.exe -NoP -NonI -EP Bypass -EncodedCommand "%PS_DISK_SPACE%"
if errorlevel 1 echo 无法自动读取磁盘空间，可直接输入要安装的盘符。
set "PS_DISK_SPACE="
exit /b 0

:select_proxy
echo 正在自动选择可用代理，请稍候...
set "proxy="
set "best_proxy="
set "best_time=999999"
set "test_url=https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/releases/latest/download/project.tar.gz"
set "PS_MEASURE_PROXY=JABzAHcAIAA9ACAAWwBTAHkAcwB0AGUAbQAuAEQAaQBhAGcAbgBvAHMAdABpAGMAcwAuAFMAdABvAHAAdwBhAHQAYwBoAF0AOgA6AFMAdABhAHIAdABOAGUAdwAoACkACgB0AHIAeQAgAHsACgAgACAAIAAgACQAbgB1AGwAbAAgAD0AIABJAG4AdgBvAGsAZQAtAFcAZQBiAFIAZQBxAHUAZQBzAHQAIAAtAFUAcgBpACAAJABlAG4AdgA6AFAAUwBfAE0ARQBBAFMAVQBSAEUAXwBVAFIATAAgAC0ATQBlAHQAaABvAGQAIABIAGUAYQBkACAALQBUAGkAbQBlAG8AdQB0AFMAZQBjACAAOAAgAC0AVQBzAGUAQgBhAHMAaQBjAFAAYQByAHMAaQBuAGcACgAgACAAIAAgACQAcwB3AC4AUwB0AG8AcAAoACkACgAgACAAIAAgAFsAaQBuAHQAXQAkAHMAdwAuAEUAbABhAHAAcwBlAGQATQBpAGwAbABpAHMAZQBjAG8AbgBkAHMACgB9ACAAYwBhAHQAYwBoACAAewAKACAAIAAgACAAOQA5ADkAOQA5ADkACgB9AA=="

for %%P in (
    https://gh-proxy.com/
    https://gh.jasonzeng.dev/
    https://git.yylx.win/
    https://wget.la/
    https://github.dpik.top/
    https://ghproxy.imciel.com/
) do (
    set "PS_MEASURE_URL=%%P%test_url%"
    for /f %%T in ('powershell.exe -NoP -NonI -EP Bypass -EncodedCommand "%PS_MEASURE_PROXY%"') do (
        set "cost=%%T"
        if !cost! LSS !best_time! (
            set "best_time=!cost!"
            set "best_proxy=%%P"
        )
    )
)

if defined best_proxy (
    set "proxy=!best_proxy!"
    echo 自动选择代理: !proxy!  延迟约 !best_time! ms
) else (
    set "proxy="
    echo 未找到可用代理，使用直连下载。
)
set "PS_MEASURE_PROXY="
set "PS_MEASURE_URL="
exit /b 0

:download_file
set "PS_DOWNLOAD_URL=%~1"
set "PS_DOWNLOAD_OUT=%~2"
set "PS_DOWNLOAD=JABFAHIAcgBvAHIAQQBjAHQAaQBvAG4AUAByAGUAZgBlAHIAZQBuAGMAZQAgAD0AIAAnAFMAdABvAHAAJwAKAEkAbgB2AG8AawBlAC0AVwBlAGIAUgBlAHEAdQBlAHMAdAAgAC0AVQByAGkAIAAkAGUAbgB2ADoAUABTAF8ARABPAFcATgBMAE8AQQBEAF8AVQBSAEwAIAAtAE8AdQB0AEYAaQBsAGUAIAAkAGUAbgB2ADoAUABTAF8ARABPAFcATgBMAE8AQQBEAF8ATwBVAFQAIAAtAFUAcwBlAEIAYQBzAGkAYwBQAGEAcgBzAGkAbgBnAA=="
powershell.exe -NoP -NonI -EP Bypass -EncodedCommand "%PS_DOWNLOAD%"
set "PS_EXIT=%errorlevel%"
set "PS_DOWNLOAD_URL="
set "PS_DOWNLOAD_OUT="
set "PS_DOWNLOAD="
exit /b %PS_EXIT%

:ensure_default_env_files
if not exist "%DIR%\xiu2" mkdir "%DIR%\xiu2" 2>nul
if not exist "%DIR%\xiu2\.env" (
    (
    echo ENVIRONMENT=dev
    echo DRIVER=~fastapi+~httpx+~websockets+~aiohttp
    ) > "%DIR%\xiu2\.env"
    echo 已创建默认配置: %DIR%\xiu2\.env
)
if not exist "%DIR%\xiu2\.env.dev" (
    (
    echo LOG_LEVEL=INFO
    echo.
    echo SUPERUSERS = ["123456"]
    echo COMMAND_START = [""]
    echo NICKNAME = ["堂堂"]
    echo DEBUG = False
    echo HOST = 0.0.0.0
    echo PORT = %PORT%
    ) > "%DIR%\xiu2\.env.dev"
    echo 已创建默认配置: %DIR%\xiu2\.env.dev
)
exit /b 0

:update_python_dependencies
python -m pip config set global.index-url https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple
if errorlevel 1 exit /b 1
python -m pip install -U pip
if errorlevel 1 exit /b 1
python -m pip install -U "nb-cli==1.5.0"
if errorlevel 1 exit /b 1
python -m pip install -U --upgrade-strategy eager wget numpy ujson Pillow wcwidth pathlib asyncio aiohttp pydantic aiofiles flask requests nonebot_plugin_apscheduler
if errorlevel 1 exit /b 1
nb driver install fastapi
if errorlevel 1 exit /b 1
nb driver install httpx
if errorlevel 1 exit /b 1
nb driver install websockets
if errorlevel 1 exit /b 1
nb adapter install onebot.v11
if errorlevel 1 exit /b 1
nb adapter install qq
if errorlevel 1 exit /b 1
if exist "%DIR%\xiu2\requirements.txt" (
    python -m pip install -U --upgrade-strategy eager -r "%DIR%\xiu2\requirements.txt"
    if errorlevel 1 exit /b 1
)
python -c "import importlib.util as u; from importlib import metadata as md; mods=[('nonebot-adapter-qq','nonebot.adapters.qq'),('nonebot-adapter-onebot','nonebot.adapters.onebot.v11'),('nonebot2','nonebot')]; [print('%%s: %%s -> %%s' %% (p, md.version(p), (lambda s: next(iter(s.submodule_search_locations)) if s and s.submodule_search_locations else (s.origin if s else 'not found'))(u.find_spec(m)))) for p,m in mods]"
exit /b %errorlevel%
