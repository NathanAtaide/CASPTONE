# `img/` — identidade visual

## Papel na arquitetura

Imagens usadas no dashboard Power BI (`Parfois Dashboard.pbip`, seção 8 do `README.md` da raiz).
O Power BI não lê esta pasta: cada imagem usada no relatório é copiada para
`Parfois Dashboard.Report/StaticResources/RegisteredResources/` e registrada em
`Parfois Dashboard.Report/definition/report.json`. Esta pasta guarda os originais e as versões
preparadas.

## Arquivos

| Arquivo | Função |
|---|---|
| `parfois.png` | Logo original, 447 × 125 px, letras brancas sobre fundo preto. O letreiro ocupa só 320 × 46 px no centro; o resto é margem preta. Por isso, inserida direto no Power BI, a caixa da imagem fica bem maior que o logo visível. |
| `parfois_logo.png` | Versão usada no relatório: recortada no letreiro (320 × 46 px, pixel 64,37 a 383,82 do original), letras brancas e fundo transparente (a transparência de cada pixel é o brilho original, então as bordas suavizadas se mantêm). No relatório aparece em 160 × 23 (metade do tamanho, nítida em telas de alta densidade), em x 20, y 44 da barra lateral de cada página. |

Para trocar o logo: gere uma nova versão recortada no conteúdo, sem margem, com o mesmo nome
`parfois_logo.png`. Copie-a para `StaticResources/RegisteredResources/` e ajuste largura e altura
do visual de imagem para a nova proporção, com o Power BI Desktop fechado.
