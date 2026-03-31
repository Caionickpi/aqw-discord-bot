# AQW Discord Bot

Bot de Discord moderno para AdventureQuest Worlds, escrito em Python com `discord.py 2.x`, slash commands, componentes de UI, scraping da Char Page pública e um sistema robusto de vínculo `Discord -> AQW`.

## O que este bot faz

- Vincula uma conta pública de AQW a um perfil do Discord.
- Gera painel público com screenshot real da Char Page.
- Cria uma arte persistente de perfil no estilo badge/blog AQW.
- Mostra resumo interativo com farms, conquistas, ultras, metas e classes.
- Rankeia membros vinculados do servidor pelas farms detectadas.
- Permite buscar itens, badges e equips dentro do perfil público.

## Principais recursos

### Vínculo e identidade

- `/vincular`
- `/desvincular`
- `/perfil`

Quando o usuário se vincula, o bot:

1. valida o nickname na Char Page pública;
2. salva o vínculo em SQLite;
3. captura o personagem via navegador headless;
4. gera uma arte PNG transparente no formato badge;
5. reaproveita essa arte nos próximos perfis para deixar a experiência mais rápida.

### Painel público com screenshot

- `/painel`

Fluxo:

1. o usuário escolhe a versão do AQW;
2. no modo clássico, informa o nick via modal;
3. o bot abre a Char Page;
4. espera o `ruffle-embed` realmente renderizar;
5. tira a screenshot do personagem;
6. envia um embed público com imagem, equips e inventário resumido.

### Comandos AQW

| Comando | Função |
|---|---|
| `/painel` | Gera um painel público do personagem com screenshot |
| `/vincular` | Vincula o Discord ao nick AQW |
| `/desvincular` | Remove a vinculação |
| `/perfil` | Mostra o perfil AQW interativo |
| `/farms` | Lista farms monitoradas detectadas |
| `/metas` | Recomenda próximos objetivos endgame |
| `/classes` | Lista classes e ranks detectados |
| `/conquistas` | Lista badges públicas |
| `/buscaritem` | Pesquisa item/classe/badge no perfil público |
| `/ultras` | Monitora progresso de ultras |
| `/comparar` | Compara dois perfis vinculados |
| `/rankingfarms` | Ranking do servidor por farms detectadas |
| `/guildaqw` | Resumo lado a lado dos vinculados no servidor |
| `/ping` | Latência do bot |
| `/help` | Menu de ajuda |

## Como o scraping funciona

### Char Page pública

O bot consulta:

- `https://account.aq.com/CharPage?id={nickname}`

Do HTML principal ele extrai:

- nome do personagem;
- título;
- level;
- faction;
- guild;
- equips visíveis;
- identificador interno `ccid`.

### Inventário

Após encontrar o `ccid`, o bot usa o endpoint público:

- `https://account.aq.com/CharPage/Inventory?ccid={ccid}`

Isso retorna o inventário público em JSON, que é usado para:

- farms;
- ultras;
- classes;
- comparação;
- busca textual.

### Badges

As badges públicas também são lidas da Char Page/endpoint correspondente e entram na análise do perfil.

### Screenshot do personagem

A imagem do personagem não vem pronta no HTML. Ela é renderizada dentro do `ruffle-embed`, então o bot:

1. abre a página em `Chrome/Chromium` headless;
2. espera o `ruffle` carregar;
3. faz polling do recorte do embed;
4. só aceita a captura quando encontra frames realmente renderizados;
5. usa essa screenshot para o painel e para a arte de perfil.

## Limitação importante: Bank

A Char Page pública da Artix expõe `Inventory` e `Badges`, mas não expõe um endpoint público de `Bank` detectável pelo bot. Por isso:

- o perfil mostra explicitamente essa limitação;
- a análise atual é baseada apenas em inventário, equips e badges públicos.

Se no futuro surgir um endpoint público estável de `Bank`, o bot pode ser expandido para isso.

## Stack

- Python 3.10+
- `discord.py 2.x`
- `requests`
- `beautifulsoup4`
- `selenium`
- `Pillow`
- `sqlite3`

## Instalação local

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

Defina:

- `DISCORD_TOKEN`
- `AQW_CHROME_BINARY` se o Chrome/Chromium não estiver em local padrão
- `CHROMEDRIVER_PATH` se quiser fixar o driver manualmente

Depois:

```bash
python bot.py
```

## Deploy Linux / systemd

Há um modelo em:

- [`systemd/aqw-charpage-bot.service.example`](D:/BotAQW/systemd/aqw-charpage-bot.service.example)

Fluxo recomendado:

1. colocar o projeto em um diretório dedicado;
2. criar um `.env` fora do Git com `DISCORD_TOKEN`;
3. configurar o serviço `systemd`;
4. habilitar com `systemctl enable --now`.

## Estrutura do projeto

```text
bot.py
requirements.txt
.env.example
systemd/
  aqw-charpage-bot.service.example
```

## Ideias já implementadas para deixar o bot mais completo

- perfil interativo com múltiplas abas;
- metas endgame recomendadas;
- ranking de farms por servidor;
- resumo da guild AQW no Discord;
- busca textual de itens/badges/equips;
- arte personalizada de perfil estilo blog AQW.

## Próximas expansões que combinam com esta base

- cache de perfil por tempo para reduzir consultas repetidas;
- `/topclasses` por servidor;
- ranking de badges públicas;
- histórico simples de progresso por usuário vinculado;
- exportação de card de perfil em imagem para redes sociais.
