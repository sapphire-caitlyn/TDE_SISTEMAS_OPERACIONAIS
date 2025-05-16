# Trabalho de análise de dados de sensoriamento utilizando pthreads

### Instruções sobre sua compilação e execução.  
- A compilação pode ser executada com o seguinte comando:

		gcc  ./program.c  -g3  -O0  -o -lpthread  ./program.o
- A execução do programa para seu funcionamento correto deve haver o arquivo CSV de origem na pasta do programa com o nome `devices.final.csv`. Os dados antes de 2024/3 estão descartados nessa versão do arquivo.
	https://drive.google.com/file/d/1ez0kVyrieUlRTLdf1KJktfWrSetorcCr/view?usp=sharing
	
### Descrição de como o CSV é carregado para o programa  
- O CSV é carregado, é calculado o nº de linhas, onde logo após é alocado a memória e os dados _parseados_ em memória.
### Descrição da forma como o programa distribui as cargas entre as threads, como por exemplo, se os dados são distribuídos entre as threads considerando um período ou dispositivo específico, ou outro critério;  
- O primeiro passo dividido por thread é a separação do array de dados em uma matriz, onde cada index é representado pelo identificador do dispositivo (Por exemplo: [sirrosteste_UCS_AMV-01] = 01) os demais dados foram descartados. As threads são separadas por quantidade de dados e quantidade de núcleos da CPU
- O segundo passo foi a separação por mes/ano, onde as threads são separadas por quantidade de dispositivos por núcleos da CPU
### Descrição da forma como as threads analisam os dados, ou seja, como cada thread identifica os valores mínimos e máximos mensais, bem como é efetuado o cálculo médio; 
-   A função inicializa um mutex global para proteger o acesso à estrutura de resultados compartilhada.
- É identificado todas as combinações únicas de mês e ano nos dados
- O número total de combinações de mês-ano é dividido entre as threads. Cada thread recebe um intervalo de índices em um array
-  **Cálculo dos Valores Estatísticos**:
    - 	Dados parcialemtne invalidos que possuem data e dispositivo considerarão 0 como dado se não estiver preenchido.
    -   Para cada sensor, a thread percorre os dados do dispositivo e verifica se o registro pertence ao mês-ano atual.
    -   Durante a iteração:
        -   **Valor Mínimo**: Atualizado sempre que um valor menor é encontrado.
        -   **Valor Máximo**: Atualizado sempre que um valor maior é encontrado.
        -   **Soma dos Valores**: Todos os valores do sensor são somados para posterior cálculo da média.
        -   **Contagem**: O número de registros válidos é contado.
        - média = soma / contagem;
	 
### Descrição da forma como as threads geram o arquivo CSV com os resultados;
- Após a execução da função de médias os resultados são escritos utilizando fprintf
### Descrição se as threads criadas são executadas em modo usuário ou núcleo;  
- As threads criadas são executadas no modo usuário
### Identificação de possíveis problemas de concorrência
 - Utilizando o processamento via mutex é possivel problemas de concorrencia, mas pela utilização de `pthread_mutex_lock` e `pthread_mutex_unlock` estes problemas não ocorrem.
