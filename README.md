# teste_engenheiro_de_dados

**PARTE 01**
Levando em conta duas fontes de dados sendo: um banco relacional e uma API de stream de dados, sua primeira missão será desenhar uma arquitetura orientada a EVENTOS para a ingestão near real-time destes dados em um datalake. Justifique brevemente sua escolha levantando os pontos positivos e negativos da solução.

Baseado no ecossistema Hadoop essa foi a solução proposta:

![Arquitetura_Eventdriven](https://github.com/thiagobtr/teste_engenheiro_de_dados/Arquitetura_EventDriven.jpg)

É uma solução de microsserviços que são independentes mas se comunicam entre si.

**Vantagens:**

* Escalável -> Podemos aumentar o numero de consumidores de acordo com a carga de dados sem afetar o envio dos eventos.
* Minimização de Falhas -> Em caso de possiveis falhas, os registros podem ser recuperados atraves do serviço de mensageria (Periodo de retenção configuravel).
* Flexibilidade -> Facilidade de integração com novos serviços de fonte de dados.
* Monitoramento e alertas -> Podemos utilizar essa arquitetura para monitorar possiveis anomalias e realizar açoes de acordo com esses eventos 
* Catalogo de 

**Desvantages:**

* Complexidade -> Em relação a infraestrtutura, precisamos garantir o monitoramento, testes a inclusão de novos processos e serviços. Como será feita o catalogo de eventos Como serão gerenciados a inclusão, alteração e exclusao de eventos?
* Segurança -> Como Temos mais serviços expostos a rede. a arquitetura está mais exposta a potencial invasores.
* Gerenciamento das transaçoes -> Devido ao seu modelo assincrono, podemos ter problemas na ordem dos eventos,inconsistencia de dados, duplicidade de registros e suporte a transaçoes ACID. 
* Gerenciamento dos dados -> Precisamos garantir o gerenciamento dos metadadados, 
