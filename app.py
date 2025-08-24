from flask import Flask, render_template, request, send_file, jsonify
import pandas as pd
import os
import sys
import io
import zipfile
from werkzeug.utils import secure_filename
import tempfile
import shutil

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB max file size
app.config['UPLOAD_FOLDER'] = tempfile.mkdtemp()

# ========== SEU SCRIPT TRATAR EXCEL - MANTIDO 100% ORIGINAL ==========
def processar_csv_trino_para_marketing_cloud(arquivo_entrada, arquivo_saida=None):
    """
    Processa CSV do Trino para deixá-lo compatível com o Marketing Cloud
    Parâmetros:
    - arquivo_entrada: caminho do CSV gerado pelo Trino
    - arquivo_saida: caminho de saída (opcional, se não informado usa o mesmo nome + '_marketing_cloud')
    """
    try:
        print(f"Lendo arquivo: {arquivo_entrada}")
        # Primeiro, identifica a coluna CPF
        try:
            header_df = pd.read_csv(arquivo_entrada, nrows=0, encoding='utf-8')
            encoding_usado = 'utf-8'
        except UnicodeDecodeError:
            print("Tentando encoding latin1...")
            header_df = pd.read_csv(arquivo_entrada, nrows=0, encoding='latin1')
            encoding_usado = 'latin1'
        # Identifica a coluna CPF
        cpf_column = None
        possible_cpf_columns = ['cpf', 'CPF', 'Cpf', 'documento', 'DOCUMENTO', 'doc', 'cpf_cnpj']
        for col in header_df.columns:
            col_clean = str(col).strip()
            if col_clean.lower() in [c.lower() for c in possible_cpf_columns]:
                cpf_column = col_clean
                break
        # Cria um dicionário de tipos para forçar CPF como string
        dtype_dict = {}
        if cpf_column:
            dtype_dict[cpf_column] = str
            print(f"Forçando coluna '{cpf_column}' como string")
        # Lê o arquivo completo
        df = pd.read_csv(
            arquivo_entrada,
            encoding=encoding_usado,
            dtype=dtype_dict,
            na_filter=False
        )
        # Remove linhas completamente vazias
        df = df.dropna(how='all')
        # Remove espaços extras dos nomes das colunas
        df.columns = df.columns.str.strip()
        # Atualiza o nome da coluna CPF após limpeza
        if cpf_column:
            for col in df.columns:
                if col.lower() in [c.lower() for c in possible_cpf_columns]:
                    cpf_column = col
                    break
        if cpf_column is None:
            print("ATENÇÃO: Não foi encontrada coluna de CPF.")
            print(f"Colunas disponíveis: {list(df.columns)}")
        else:
            # RENOMEIA A COLUNA CPF PARA "CPF" (como no arquivo que funciona)
            df = df.rename(columns={cpf_column: 'CPF'})
            cpf_column = 'CPF'
            # Move a coluna CPF para primeira posição
            if cpf_column != df.columns[0]:
                print(f"Movendo coluna '{cpf_column}' para primeira posição...")
                cols = df.columns.tolist()
                cols.remove(cpf_column)
                cols.insert(0, cpf_column)
                df = df[cols]
            # Processa a coluna CPF
            print(f"Processando coluna CPF: {cpf_column}")
            # Garante que seja string
            df[cpf_column] = df[cpf_column].astype(str)
            # Remove .0 do final se existir
            df[cpf_column] = df[cpf_column].str.replace(r'\.0+$', '', regex=True)
            # Remove pontos, traços e espaços
            df[cpf_column] = df[cpf_column].str.replace(r'[.\-\s]', '', regex=True)
            # Remove qualquer caractere não numérico
            df[cpf_column] = df[cpf_column].str.replace(r'[^0-9]', '', regex=True)
            # Remove linhas com CPF vazio
            df = df[df[cpf_column].str.len() > 0]
            # Filtra CPFs válidos
            df = df[df[cpf_column].str.len() <= 11]
            # Completa com zeros à esquerda
            df[cpf_column] = df[cpf_column].str.zfill(11)
            print(f"Amostra processada: {df[cpf_column].head(3).tolist()}")
        # Define o arquivo de saída
        if arquivo_saida is None:
            nome_base = os.path.splitext(arquivo_entrada)[0]
            arquivo_saida = f"{nome_base}_marketing_cloud.csv"
        print(f"Salvando arquivo processado: {arquivo_saida}")
        print(f"Colunas finais: {list(df.columns)}")
        # Salva no formato EXATO que funciona no Marketing Cloud
        df.to_csv(
            arquivo_saida,
            index=False,
            encoding='utf-8',
            sep=';', # PONTO E VÍRGULA (principal diferença!)
            lineterminator='\n',
            quotechar='"',
            quoting=0, # QUOTE_NONE - sem aspas!
            float_format='%.0f',
            escapechar=None
        )
        print("✅ Arquivo processado com sucesso!")
        print(f"📋 Total de registros: {len(df)}")
        print(f"📂 Arquivo salvo em: {arquivo_saida}")
        # Verifica o arquivo criado
        print("\n🔍 Verificando arquivo criado:")
        with open(arquivo_saida, 'r', encoding='utf-8') as f:
            primeiras_linhas = [f.readline().strip() for _ in range(5)]
            print("Primeiras linhas do arquivo:")
            for i, linha in enumerate(primeiras_linhas):
                if linha:
                    print(f"Linha {i+1}: {linha}")
        return arquivo_saida
    except Exception as e:
        print(f"❌ Erro ao processar arquivo: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

# ========== SEU SCRIPT QUEBRAR EXCEL - MANTIDO 100% ORIGINAL ==========
def quebrar_excel_por_linhas(arquivo_entrada, tam_max_linhas=2400000):
    """
    Quebra o arquivo CSV por número de linhas
    """
    try:
        # Tentar ler como CSV primeiro
        df_lista = pd.read_csv(arquivo_entrada, encoding='utf-8')
    except UnicodeDecodeError:
        try:
            df_lista = pd.read_csv(arquivo_entrada, encoding='latin1')
        except:
            df_lista = pd.read_csv(arquivo_entrada, encoding='iso-8859-1')
    
    # Dividir o DataFrame em subconjuntos
    subconjuntos = [df_lista[i:i + tam_max_linhas] for i in range(0, len(df_lista), tam_max_linhas)]
    
    arquivos_criados = []
    nome_base = os.path.splitext(os.path.basename(arquivo_entrada))[0]
    
    # Salvar cada subconjunto em um arquivo separado
    for i, subconjunto in enumerate(subconjuntos):
        nome_arquivo = os.path.join(app.config['UPLOAD_FOLDER'], f'{nome_base}_{i + 1}.csv')
        subconjunto.to_csv(nome_arquivo, index=False)
        arquivos_criados.append(nome_arquivo)
        print(f"Arquivo salvo: {nome_arquivo} - {len(subconjunto)} linhas")
    
    print(f"Processo concluído! Arquivo dividido em {len(subconjuntos)} partes.")
    return arquivos_criados

# ========== NOVA FUNÇÃO - QUEBRAR POR TAMANHO ==========
def quebrar_excel_por_tamanho(arquivo_entrada, max_size_mb=75):
    """
    Quebra o arquivo CSV por tamanho (em MB)
    """
    max_size_bytes = max_size_mb * 1024 * 1024
    
    # Ler o arquivo CSV com tratamento de encoding
    try:
        df = pd.read_csv(arquivo_entrada, encoding='utf-8')
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(arquivo_entrada, encoding='latin1')
        except:
            df = pd.read_csv(arquivo_entrada, encoding='iso-8859-1')
    
    arquivos_criados = []
    nome_base = os.path.splitext(os.path.basename(arquivo_entrada))[0]
    
    # Estimar o tamanho de cada linha
    temp_file = os.path.join(app.config['UPLOAD_FOLDER'], 'temp_estimate.csv')
    df.head(1000).to_csv(temp_file, index=False)
    size_1000_rows = os.path.getsize(temp_file)
    avg_row_size = size_1000_rows / 1000
    os.remove(temp_file)
    
    # Calcular quantas linhas cabem em max_size_mb
    estimated_rows_per_file = int(max_size_bytes / avg_row_size * 0.95)  # 95% para margem de segurança
    
    # Dividir o DataFrame
    inicio = 0
    parte = 1
    
    while inicio < len(df):
        fim = min(inicio + estimated_rows_per_file, len(df))
        subconjunto = df[inicio:fim]
        
        # Salvar e verificar o tamanho
        nome_arquivo = os.path.join(app.config['UPLOAD_FOLDER'], f'{nome_base}_parte{parte}.csv')
        subconjunto.to_csv(nome_arquivo, index=False)
        
        # Se o arquivo ficou maior que o limite, reduzir o número de linhas
        if os.path.getsize(nome_arquivo) > max_size_bytes and fim - inicio > 1:
            os.remove(nome_arquivo)
            # Reduzir o número de linhas e tentar novamente
            estimated_rows_per_file = int(estimated_rows_per_file * 0.9)
            fim = min(inicio + estimated_rows_per_file, len(df))
            subconjunto = df[inicio:fim]
            subconjunto.to_csv(nome_arquivo, index=False)
        
        arquivos_criados.append(nome_arquivo)
        tamanho_mb = os.path.getsize(nome_arquivo) / (1024 * 1024)
        print(f"Arquivo salvo: {nome_arquivo} - {len(subconjunto)} linhas - {tamanho_mb:.2f} MB")
        
        inicio = fim
        parte += 1
    
    print(f"Processo concluído! Arquivo dividido em {len(arquivos_criados)} partes.")
    return arquivos_criados

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/processar', methods=['POST'])
def processar():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'Nenhum arquivo enviado'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'Nenhum arquivo selecionado'}), 400
        
        acao = request.form.get('acao')
        
        # Salvar arquivo temporariamente
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Log para debug
        print(f"Arquivo recebido: {filename}")
        print(f"Tamanho do arquivo: {os.path.getsize(filepath)} bytes")
        print(f"Ação solicitada: {acao}")
        
        resultado_arquivos = []
        
        if acao == 'marketing_cloud':
            # Processar para Marketing Cloud
            arquivo_processado = processar_csv_trino_para_marketing_cloud(filepath)
            if arquivo_processado:
                resultado_arquivos = [arquivo_processado]
            else:
                return jsonify({'error': 'Erro ao processar arquivo'}), 500
                
        elif acao == 'quebrar_linhas':
            # Quebrar por linhas
            max_linhas = int(request.form.get('max_linhas', 2400000))
            resultado_arquivos = quebrar_excel_por_linhas(filepath, max_linhas)
            
        elif acao == 'quebrar_tamanho':
            # Quebrar por tamanho
            max_mb = int(request.form.get('max_mb', 75))
            resultado_arquivos = quebrar_excel_por_tamanho(filepath, max_mb)
        
        else:
            return jsonify({'error': 'Ação inválida'}), 400
        
        # Se temos múltiplos arquivos, criar um ZIP
        if len(resultado_arquivos) > 1:
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for arquivo in resultado_arquivos:
                    zip_file.write(arquivo, os.path.basename(arquivo))
            
            zip_buffer.seek(0)
            
            # Limpar arquivos temporários
            for arquivo in resultado_arquivos:
                if os.path.exists(arquivo):
                    os.remove(arquivo)
            if os.path.exists(filepath):
                os.remove(filepath)
            
            return send_file(
                zip_buffer,
                mimetype='application/zip',
                as_attachment=True,
                download_name=f'{os.path.splitext(filename)[0]}_processado.zip'
            )
        
        # Se temos apenas um arquivo, enviar diretamente
        elif len(resultado_arquivos) == 1:
            # Ler o arquivo para enviar
            with open(resultado_arquivos[0], 'rb') as f:
                file_data = io.BytesIO(f.read())
            
            # Limpar arquivos temporários
            if os.path.exists(resultado_arquivos[0]):
                os.remove(resultado_arquivos[0])
            if os.path.exists(filepath):
                os.remove(filepath)
            
            file_data.seek(0)
            return send_file(
                file_data,
                mimetype='text/csv',
                as_attachment=True,
                download_name=os.path.basename(resultado_arquivos[0])
            )
        
        else:
            return jsonify({'error': 'Nenhum arquivo foi gerado'}), 500
            
    except Exception as e:
        print(f"Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    os.makedirs('templates', exist_ok=True)
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))