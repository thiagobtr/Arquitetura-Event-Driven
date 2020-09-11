# teste_engenheiro_de_dados

**PARTE 01**
Levando em conta duas fontes de dados sendo: um banco relacional e uma API de stream de dados, sua primeira missão será desenhar uma arquitetura orientada a EVENTOS para a ingestão near real-time destes dados em um datalake. Justifique brevemente sua escolha levantando os pontos positivos e negativos da solução.

Baseado no ecossistema Hadoop essa foi a solução proposta:

![Arquitetura_Eventdriven](https://github.com/thiagobtr/teste_engenheiro_de_dados/blob/master/Arquitetura_EventDriven.jpg)

É uma solução de microsserviços que são independentes mas se comunicam entre si.
Utilização do Kafka, serviço de mensageria composto por um cluster broker (Topicos e partiçoes), onde os dados podem ser replicados.
O Ambari seria a ferramenta para gerenciamento e monitoramento do cluster e o Zookeeper para a coordenação dos serviços nesse ambiente.
Apache Ranger e Atlas seria a solução para gerenciamento dos metadados, classificação, criptografia e acesso a dados sensiveis.
Esta arquitetura estaria localizada em um ambiente em nuvem.

**Vantagens:**

* Escalabilidade -> Podemos aumentar ou diminuir o numero de maquinas de acordo com a carga de dados sem afetar o envio dos eventos.
* Minimização de Falhas -> Em caso de possiveis falhas ou interrupção dos serviços, os registros ficam armazenados (Periodo de retenção configuravel) na camada de ingestão.
* Flexibilidade -> Facilidade de integração com novos serviços.  Os eventos podem ser separados por topicos, facilitando a administração e a integração com o destino. 
* Monitoramento e alertas -> Podemos utilizar essa arquitetura para monitorar possiveis anomalias e realizar açoes de acordo com esses eventos 
* Auditoria -> Maior controle para reproduzir eventos para detecção de possiveis fraudes

**Desvantages:**

* Complexidade -> Em relação a infraestrutura, precisamos garantir o monitoramento, testes, administração dos recursos e a inclusão de novos processos e serviços.
* Segurança -> Como temos mais serviços expostos a rede, a arquitetura está mais exposta a potencial invasores.
* Gerenciamento das transaçoes -> Devido ao seu modelo assincrono, podemos ter problemas na ordem dos eventos, inconsistencia de dados, duplicidade de registros e suporte a transaçoes ACID.
* Gerenciamento dos dados -> Precisamos garantir o gerenciamento dos metadadados, catálogo e administraçãos dos eventos.

PARTE 02
Seu objetivo é criar um script PYSPARK, que gere um arquivo único de output, utilizando os 3 arquivos em csv (station.csv, trip.csv e weather.csv), contendo todos os dados de viagens, acrescidos de:
Latitude/Longitude da estação de início e de fim.
Uma coluna com o nome "long_trip" de valor booleano sendo 'true' para viagens superiores a 10 minutos.
Condição meteorológica no dia da viagem (coluna events da tabela weather).
Uma coluna com o nome "age_range" sendo o valor de 1 para pessoas de 0-16 anos, 2 de 17-25, 3 de 26-50 e 4 para 50+.
O serviço de execução fica a seu critério, podendo utilizar tanto serviços locais como serviços em cloud. Justificar brevemente o serviço escolhido (EMR, Glue, Zeppelin, etc.) e o formato do arquivo final. Incluir também o passo a passo de como executar o script.

Serviço escolhido: 
** ambiente local - Jupyter notebook **
** Motivo ** facil reprodução e interatividade.
** Arquivo de saida ** formato csv. 
Este arquivo de saida pode ser utilizado em um processo de ETL para carregar um banco de dados, data lake, ou mesmo servir uma aplicação de self-BI


