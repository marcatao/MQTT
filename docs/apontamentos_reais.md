# Apontamentos reais pelo IoT (MQTT.dbo.IOT001 → OEE.dbo.apontamentos_estado)

Decidido com o usuário em 12/11/2026. O integrador gera, a partir das leituras de contagem gravadas na `IOT001`, a
**timeline** de cada máquina no portal OEE (produzindo × microparada × macroparada) e atualiza a situação da máquina.
O objetivo é dizer **se a máquina trabalhou ou parou**. Depois, o operador vai apontar o motivo da parada no portal
(troca 98 por Setup, Manutenção etc.).

## Origem dos dados (portal OEE, repositório marcatao/OEE, só leitura do código)

- `OEE.dbo.maquinas`: `id`, `codigo`, `nome`, `ativo` (bit), `d_e_l_e_t_` (`'*'` = excluída no Protheus),
  `micro_parada_s`, `macro_parada_s` (int, segundos, `macro > micro`), `iot1_dispositivo_id`..`iot3_dispositivo_id`
  (FK `dispositivos_iot.id`), `iot1_canal`..`iot3_canal` (`chd1_value`..`chd6_value`), `iot1_tipo`..`iot3_tipo`
  (`producao` | `refugo` | NULL), `situacao` (`produzindo` | `parada` | NULL, CHECK), `situacao_desde` (UTC),
  `situacao_legenda_id` (FK legendas), `ordem_producao_id` (FK ordens_producao, OP atual), `atualizado_em`.
- `OEE.dbo.dispositivos_iot`: `id`, `codigo` (= `device_id` da IOT001), `ativo`.
- `OEE.dbo.legendas`: `id`, `codigo` (varchar 12), `descricao`, `tipo` (`produzindo` | `parada` | `planejada` |
  `desligada`), `ativo`. Usadas pelo integrador: **`'2'` Produção Normal**, **`'99'` Microparada**, **`'98'` Macro
  parada** (já cadastradas no portal; se faltar alguma, o ciclo não aponta e avisa no log); **`'97'` Sem
  apontamento IOT** para os períodos sem leitura (ver abaixo); **`'96'` Fora de turno** para parada fora do horário
  de trabalho.
- `OEE.dbo.apontamentos_estado`: `id` (identity), `maquina_id`, `legenda_id`, `ordem_producao_id` (NULL ok),
  `inicio` (datetime, UTC), `fim` (NULL = ainda neste estado; CHECK `fim IS NULL OR fim > inicio`), `referencia`
  (40), `observacao` (500), `criado_em`, `criado_por_id`, `atualizado_em`, `atualizado_por_id` (usuários; o integrador
  deixa NULL).
- `OEE.dbo.ordens_producao`: `id`, `c2_datrf`, `c2_status`, `c2_quje`, `status` (`aberta` | `andamento` | `parada` |
  `fechada`).

Máquinas processadas: `ativo = 1`, `d_e_l_e_t_ <> '*'`, `micro_parada_s` e `macro_parada_s` preenchidos e pelo menos
um canal IoT com dispositivo ativo e tipo `producao` (tipo NULL conta como produção; `refugo` não entra no estado).
Máquina sem micro/macro: ignorada com 1 INFO por sessão.

## Regra do estado

Os canais são **contadores** (o DigiRail manda uma leitura a cada ~10 s mesmo parado). Uma **produção** é uma leitura
em que o valor do canal subiu em relação à leitura anterior do mesmo canal (contador zerado ou valor menor = nova
base, não é produção). Com mais de um canal de produção, vale a produção de qualquer um deles.

O **intervalo** é o tempo entre duas produções seguidas (`p` = anterior, `t` = seguinte, `gap = t − p`):

| Intervalo | Período `[p, t)` | Legenda |
| --- | --- | --- |
| `gap < micro_parada_s` | produzindo | **2** Produção Normal |
| `micro_parada_s ≤ gap < macro_parada_s` | microparada | **99** Microparada |
| `gap ≥ macro_parada_s` | parada | **98** Macro parada |

A parada começa na última produção `p` e termina na produção seguinte `t` (a partir de `t` a máquina volta a
"produzindo"). Se até agora não houve nova produção, o intervalo em aberto é `agora − p` e a mesma tabela vale: o
apontamento em aberto vira 99 quando passa da micro e 98 quando passa da macro (o início continua em `p`).

## Sem leitura do aparelho: 97 Sem apontamento IOT (decidido em 12/11/2026)

- **Sem leitura** = nenhuma linha na IOT001 de nenhum dos dispositivos da máquina por mais de
  `[apontamentos] sem_leitura_segundos` (padrão **120 s**; o DigiRail manda a cada ~10 s). O trecho vai da última
  leitura antes do buraco até a primeira leitura depois dele (ou até agora, se o aparelho ainda não voltou; ou do
  início da janela até a primeira leitura) e recebe a legenda **97 Sem apontamento IOT** (código em
  `[apontamentos] legenda_sem_iot = "97"`; situação da máquina = parada).
- Com buracos sem leitura, o intervalo entre produções conta **só o tempo com leitura**: para `[p, t)`, o intervalo
  observado é `(t − p) − tempo sem leitura dentro de [p, t)`, e os trechos com leitura dessa faixa são classificados
  (2 / 99 / 98) por esse intervalo observado. O mesmo vale para o intervalo em aberto (até agora).
- Máquina sem nenhuma leitura na janela: a janela inteira é 97 (antes era 98).
- **Correção retroativa**: se depois chegam leituras na IOT001 com `ts_unix` dentro de um apontamento 97 do integrador
  (o aparelho reenvia a memória quando volta), o trecho é recalculado com os dados reais. A cada ciclo, para cada
  máquina, procura o apontamento 97 do integrador mais antigo dos últimos `[apontamentos] dias_correcao` (padrão
  **7**) dias que tem leitura dentro dele (`ts_unix > inicio` e `<= fim`); se houver, a janela daquele ciclo começa
  às 00:00 local do dia dele (em vez de ontem/hoje). Sem leitura nova, a janela não muda (não relê 7 dias à toa).
- Legenda 97 ausente no portal: AVISO (1 vez até resolver) e os trechos sem leitura voltam a ser tratados como antes
  (sem leitura = sem produção → 99/98), para não parar a timeline.

## Fora de turno: 96 (decidido em 13/11/2026)

A máquina tem no portal um **calendário** e um **horário de trabalho**:

- `maquinas.calendario_id` → `calendarios` (`id`, `ano`, `ativo`) e `calendario_dias` (`calendario_id`, `data` date,
  `tipo`: `feriado` | `ponte` | `folga` = não útil; `compensacao` | `util` = útil). Padrão: dia útil se o dia da
  semana está nos dias de trabalho da máquina; uma exceção em `calendario_dias` para a data **manda no dia**
  (regra do portal em `App\Services\Calendario\DiasUteis::trabalha`). Sem calendário: só pelos dias da semana.
- `maquinas.horarios_semana` (varchar 200, JSON `{"1":["05:00","14:48"],…}`, dia ISO 1 = segunda … 7 = domingo,
  HH:MM local; fim antes do início = vira o dia). Sem `horarios_semana` (ou coluna ausente): `dias_semana`
  (ex.: `"12345"`) + `jornada_inicio`/`jornada_fim` para todos esses dias (regra de `App\Support\JornadaSemanal::de`).
  Dia útil por exceção (compensação/dia útil extra) que não tem horário próprio usa `jornada_inicio`/`jornada_fim`
  (ou o horário do primeiro dia de trabalho).
- **Dentro do turno** = dentro de `[início, fim)` de um dia de trabalho útil pelo calendário (o turno pertence ao dia
  em que começa; turno que vira o dia termina no dia seguinte).

Regra: todo trecho **parado** (98, 99 e 97) que cair **fora do turno** vira **96 Fora de turno**; dentro do turno
continua como está. Produção (2) fora do turno continua 2. Os trechos são cortados nas bordas do turno. Exemplo do
usuário: turno começa 05:00, máquina parada das 00:00 às 05:22 e depois produzindo → **96** 00:00–04:59:59, **98**
05:00–05:22, depois **2**. A classificação 99/98 continua pelo intervalo inteiro da parada (só a parte dentro do turno
fica 99/98).

- Máquina sem horário de trabalho cadastrado: não há 96 (tudo como antes) e a tela mostra "sem jornada".
- Legenda 96 ausente no portal: AVISO (1 vez até resolver) e segue sem 96.
- Código em `[apontamentos] legenda_fora_turno = "96"`. Situação da máquina com 96 = parada.

## Diagnóstico antes de apontar: 97 se o aparelho não responde (decidido em 13/11/2026)

Antes de calcular os apontamentos, o ciclo manda o **diagnóstico** (`diag`, o mesmo da aba Clientes) a cada
dispositivo das máquinas elegíveis e espera as respostas (até `[dispositivos] timeout_comando_segundos`, 15 s, todos
em paralelo). Se um dispositivo **não responde**, a parada **em aberto** (o último trecho, da última produção até
agora) das máquinas dele vira **97 Sem apontamento IOT** (fora do turno continua 96). Quando ele voltar a responder,
o trecho volta a ser calculado normalmente (reconciliação). Sem MQTT conectado não há como perguntar: o ciclo segue
sem essa regra (aviso na linha da máquina). Diagnóstico que responde não muda nada. Atenção: o tópico de confirmação
precisa estar certo na aba Clientes ("Tópicos…"), senão o aparelho parece não responder.

## Recalcular o dia de uma máquina (decidido em 13/11/2026)

Na aba, cada máquina tem o botão **Recalcular…**: escolhe o dia (de hoje até `dias_correcao` dias atrás), mostra a
confirmação "Os apontamentos MANUAIS desta máquina a partir de dd/mm serão apagados e a timeline será refeita pelo
IoT" e, confirmado, o integrador refaz a máquina desde 00:00 local daquele dia até agora (mesma reconciliação, com
`ini` = esse dia), sem esperar o ciclo. Resposta no sinal `recalculo_concluido` e no log.

## Apontamentos únicos e encadeados (regras do portal, `HorarioApontamento`)

- Períodos seguidos com o mesmo estado formam **um único apontamento**, que vai crescendo; o último fica em aberto
  (`fim` NULL).
- Um apontamento não passa de um dia no fuso `America/Sao_Paulo`: na meia-noite local fecha às 23:59:59 e continua
  num novo apontamento às 00:00:00 com o mesmo estado.
- Encadeados: o seguinte começa 1 s depois do fim do anterior (`fim = próximo_inicio − 1 s`). Precisão de segundos;
  trecho com menos de 2 s é absorvido pelo anterior (respeita `fim > inicio`).
- Datas gravadas em UTC (`datetime`, sem fração).

## Reconciliação (a cada ciclo)

Janela por máquina: de **ontem 00:00 local** (se a máquina já tem apontamento do integrador ontem) ou de **hoje 00:00
local** (primeira execução) até agora. Leituras lidas a partir de `início_da_janela − macro_parada_s` (para saber a
última produção antes da janela; sem produção nesse trecho, o período até a primeira produção é macroparada).

1. Calcula os segmentos desejados na janela (regra acima, dividida por dia).
2. Compara com os apontamentos existentes da máquina que cruzam a janela:
   - **Do integrador** (marcados com `observacao` começando por `[IoT]`): o de mesmo `inicio` é reaproveitado
     (atualiza `fim` e `legenda_id`); os que sobram são reaproveitados ou excluídos; os que faltam são inseridos.
     Se o operador trocou a legenda de uma parada (98/99 → Setup, Manutenção...) e o trecho continua sendo parada, a
     legenda do operador é **mantida**; se o trecho virou produção, volta para 2.
   - **Manuais** (sem a marca): o **IoT prevalece**. Manual que cruza um segmento do integrador tem `inicio`/`fim`
     ajustados para fora do trecho; manual inteiramente coberto é excluído. Cada ajuste/exclusão vai para o log
     (AVISO) com máquina, legenda e horário.
3. Apontamento novo leva a OP atual da máquina (`maquinas.ordem_producao_id`); reaproveitado mantém a dele.
4. Tudo de uma máquina numa transação.

## Máquina e OP (o que o portal faz em `SituacaoMaquina` e `StatusOrdemProducao`)

- Depois da reconciliação, o apontamento **mais recente** da máquina (`ORDER BY inicio DESC, id DESC`) dá:
  `situacao` = `produzindo` se a legenda é do tipo `produzindo`, senão `parada`; `situacao_desde` = `inicio`;
  `situacao_legenda_id` = `legenda_id`; `ordem_producao_id` = a OP do apontamento. Só grava se algo mudou
  (`atualizado_em` = agora UTC).
- Status das OPs envolvidas (dos apontamentos criados, alterados ou excluídos): `fechada` se `c2_datrf` preenchido;
  `parada` se `c2_status = 'U'` ou o apontamento em aberto mais recente da OP não é `produzindo`; `andamento` se há
  apontamento em aberto produzindo, `c2_quje > 0` ou a OP tem algum apontamento; senão `aberta`. Só grava se mudou.

## Execução e tela

- Thread própria no core (sem Qt), a cada **3 min** (`[apontamentos] minutos = 3`; a tela pode mudar, valor salvo em
  `QSettings`). Botão "Executar agora". Roda sempre que o SQL Server está configurado (com senha), mesmo sem MQTT.
- Sem banco/tabela/permissão do OEE: 1 INFO e tenta de novo no próximo ciclo (não é erro). Falha de SQL no meio do
  ciclo: erro categoria `"apontamentos"`, rollback da máquina, segue para a próxima.
- Aba **Apontamentos reais** (ao lado de Clientes): periodicidade, Executar agora, última/próxima execução, e uma
  linha por máquina com Máquina, Dispositivo/canal, Micro/Macro, Estado atual (legenda), Desde, Última produção,
  Apontamentos hoje e Última mensagem.
- Permissões: `sql/permissoes_oee.sql` (SELECT em maquinas, dispositivos_iot, legendas, ordens_producao;
  SELECT/INSERT/UPDATE/DELETE em apontamentos_estado; UPDATE em maquinas e ordens_producao) e SELECT na IOT001.
