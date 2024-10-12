# Snowflake Native Applicationのひな形
`VS Code`の利用を前提とした開発環境です。

## 主要な採用ツール
- パッケージ管理ツール: Poetry
- Linter / Formatter: Ruff
    - `pyproject.toml`の設定で管理できる
    - 下記が入ってかつRust製で高速
        - Black
        - Flake8
        - isort
- Type Checker: mypy
- 単体テスト: pytest
- タスクランナー: taskipy
- pre-commit
    - コミット前にRuffやmypyが自動実行されるようにするgitのhook

## 事前にインストールしておくツール
インストール手順は[Notionページ](https://www.notion.so/PP-Native-APP-93142f1c62b44e37a15daf6536e2e9ed?pvs=4)を参考に。
- VS Code
    - 各種拡張機能: このリポジトリをクローンして開き、検索窓で「`@recommended`」と入力し全てインストール。
- Poetry
    - Notionページの推奨設定を要参照。

## 本プロジェクトの使い方
### 1. Poetry
```bash
# パッケージを仮想環境にインストール
# グローバル環境には一切影響はありません。
poetry install
# poetryのshell環境に入る
# これをやらない場合は、poetry runを全コマンドの最初につける必要あり。
# VS Codeのターミナルで仮想環境の自動アクティベートが有効な場合は不要。
poetry shell
# pre-commitの準備
# poetry install後に使えるようになるコマンドです。
pre-commit install
```
### 2. Snowflake
#### 接続情報の設定
`.snowflake/config.toml.exmaple`を`./snowflake/config.toml`ファイルに名称を変更し、
自分のテスト用に払い出されたユーザ接続情報を定義。

Snowflake CLIの`snow`コマンドの前に`task`を追記することで、その接続情報が自動的に使用されるようになります。
```bash
task snow app run
# taskを使わない場合
snow app run -c some_connection
```

#### ログファイル
`task`で実行する`snow`コマンドのログは、`.snowflake/logs`配下に保存されます。
`.snowflake/config.toml`の`[cli.logs]`項で設定変更可能です。

## 本環境の作成過程
記録として残しておきます。
### poetry環境初期化
```bash
poetry new .
```
poetryのパッケージング機能は不要なため、`pyproject.toml`の`[tool.poetry]`項目を下記のみに変更。
```toml
[tool.poetry]
package-mode = false
```

### Native Appのパッケージ追加
```bash
# snowコマンドのパッケージをインストール
# 2.7以上かつ2.8未満で固定
poetry add snowflake-cli-labs@~2.7
# poetryのシェル環境に切り替え
poetry shell
# ネイティブアプリパッケージを作成
# カレントディレクトリには作成不可なため、適当にサブフォルダ指定
snow app init snowflake_native_app
# アプリパッケージを一段上の階層に移動
rm ./snowflake_native_app/README.md
mv ./snowflake_native_app/* .
# 隠しファイルが移動されないため
mv ./snowflake_native_app/.gitignore .
rmdir ./snowflake_native_app/
```

### 開発環境に必要なパッケージ追加
```bash
poetry add --group dev ruff mypy pytest taskipy pre-commit
# snowflakeのNativeアプリのタイプヒント
poetry add --group dev snowflake-snowpark-python
poetry add --group dev snowflake-native-apps-permission-stub
```
各種設定項目を`pyproject.toml`と`.vscode/settings.json`に追記。
`ruff`と`mypy`のVS Code拡張機能は、環境にインストールされたパッケージの方を参照するようにしている。

### pre-commitの設定
```bash
touch .pre-commit-config.yaml
```
`.pre-commit-config.yaml`に`ruff`と`mypy`のフックを記述。
