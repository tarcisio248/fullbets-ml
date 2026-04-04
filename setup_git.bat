@echo off
echo ============================================================
echo  FULLBETS ML - Configurar Git e push para GitHub
echo ============================================================
echo.

REM Verificar se o Git está instalado
git --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERRO: Git não instalado.
    echo Baixe em: https://git-scm.com/download/win
    echo Instale e execute este script novamente.
    pause
    exit /b 1
)

echo Git encontrado!
echo.

REM Navegar para a pasta do projeto
cd /d "C:\Users\Tarcisio\PycharmProjects\PythonProject3"

REM Configurar nome e email do Git (só na primeira vez)
git config --global user.name "tarcisio248"
git config --global user.email "tarcisioqueiroz248@gmail.com"

REM Inicializar repositório local (se ainda não for um repo Git)
if not exist ".git" (
    echo Inicializando repositório Git...
    git init
    git checkout -b main
)

REM Adicionar remote origin (remove se já existir)
git remote remove origin 2>nul
git remote add origin https://github.com/tarcisio248/fullbets-ml.git

REM Buscar o estado atual do repositório remoto
echo Conectando ao GitHub...
git fetch origin

REM Adicionar todos os arquivos (exceto os do .gitignore)
echo.
echo Adicionando arquivos...
git add pipeline_fullbets.py
git add model_trainer.py
git add gerar_pagina.py
git add requirements.txt
git add .gitignore

REM Criar pastas necessárias e adicionar arquivos dentro delas
if exist ".github\workflows\pipeline_diario.yml" (
    git add .github\workflows\pipeline_diario.yml
)
if exist "docs\index.html" (
    git add docs\index.html
)
if exist "docs\sinais.json" (
    git add docs\sinais.json
)

REM Mostrar o que vai ser commitado
echo.
echo Arquivos prontos para commit:
git status --short

REM Fazer o commit
echo.
echo Fazendo commit...
git commit -m "FULLBETS ML pipeline completo - v1"

REM Push para o GitHub
echo.
echo Fazendo push para GitHub...
echo (O Windows vai pedir suas credenciais do GitHub)
echo Use: usuario = tarcisio248
echo      senha   = seu Personal Access Token (nao a senha do GitHub)
echo.
git push -u origin main

echo.
echo ============================================================
if %errorlevel% equ 0 (
    echo  SUCESSO! Arquivos enviados para:
    echo  https://github.com/tarcisio248/fullbets-ml
) else (
    echo  ERRO no push. Verifique suas credenciais.
)
echo ============================================================
pause
