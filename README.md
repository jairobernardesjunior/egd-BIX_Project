# BIX_teste_pratico
Temos três fontes com seguintes dados:
•	Banco PostgreSQL com dados de vendas
•	API com dados de funcionários
•	Arquivo parquet com dados de categoria

Recupera os dados de vendas da tabela venda do Banco de Dados PostgreSQL na nuvem, conectado remotamente, e salva esses dados na tabela vendas em um banco de dados postgreSQL local. Consome a API de funcionários  (https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id=1) variando o id de 1 a 9 e insere esses dados na tabela funcionarios no banco de dados postgreSQL local. Baixa o arquivo de categorias através do link https://storage.googleapis.com/challenge_junior/categoria.parquet e insere esses dados na tabela categorias no banco de dados postgreSQL local. Todo esse processo deve se repetir diariamente para que novos dados sejam inseridos nessas três tabelas, complementando-as. Esse processo  será orquestrado por um pipeline responsável pelo start e processamento diário.