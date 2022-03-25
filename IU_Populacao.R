library(dplyr)
library(readr)
library(stringr)
library(lubridate)
library(tidyr)
library(sf)
library(httr)
library(ows4R)
library(writexl)


# -------------------------------------------------------------------------------------------------------------------- #
#### 00 - dados brutos ####
# -------------------------------------------------------------------------------------------------------------------- #

##### função para ler camadas WFS GeoSampa #####
GeoSampaWFS <- "http://wfs.geosampa.prefeitura.sp.gov.br/geoserver/geoportal/wfs?"
bwk_client <- WFSClient$new(GeoSampaWFS, serviceVersion = "2.0.0")

GeoSampaWFS_lista <- bwk_client$getFeatureTypes() |>
  purrr::map_chr(function(x){x$getName()}) |> sort() |> tibble::enframe()

View(GeoSampaWFS_lista)

le_WFS <- function( camada , adicional = "" ){
  
  link <- paste0( 
    "http://wfs.geosampa.prefeitura.sp.gov.br/geoserver/geoportal/wfs?" ,  
    "service=WFS",
    "&version=2.0.0",
    "&request=GetFeature",
    "&typename=",
    camada,
    "&outputFormat=application/json",
    adicional
  )
  
  return(st_read(link))
  
  
}

# rodar caso esteja em proxy -> httr::set_config( use_proxy(url="your.proxy.ip", port="port", username="user",password="password") )

dltemp <- tempfile()

##### 2000-2050 - microdados SEADE #####
link <- "https://repositorio.seade.gov.br/dataset/09975945-64f1-48cc-9f4d-72f11ddd5e6f/resource/8f0d769a-a8ec-4c3d-a456-bf281c65ccb6/download/evolucao_msp_sexo_idade2000_50.csv"
download.file( link, dltemp, mode = "wb" )
IUPop_01_popDistrito <- read_csv2( dltemp , locale = locale( encoding = "ISO8859-1" ) ) |>
  mutate(
          # removendo acentos e deixando em caixa alta para junção posterior
          distrito = toupper( stringi::stri_trans_general( distrito , "Latin-ASCII") )
        ) |>
  # abrir em colunas
  pivot_wider( names_from = c( ano , sexo , faixa_etaria ) , values_from = populacao )

##### macroárea #####
# arquivo local pois problemas topológicos impossibilitam, por enquanto, uso do WFS
IUPop_02_macroarea <- le_WFS( "geoportal:pde2014_v_mcrar_02_map" ) |>
  select( sg_macro_divisao_pde ) |>
  mutate( area_macroarea_ha = as.numeric( st_area( geom ) ) ) |>
  st_make_valid() |>
  st_transform(31983) |>
  st_cast( "POLYGON" )

IUPop_03_distrito <- le_WFS( "geoportal:distrito_municipal" ) |>
  mutate( 
          cd_distrito_municipal = str_pad( cd_distrito_municipal , width = 2 , side = "left" , pad = "0" ),
          ) |>
  left_join( le_WFS( "geoportal:subprefeitura" ) |> st_drop_geometry() |> select( cd_identificador_subprefeitura , nm_subprefeitura ) ) |>
  select( contains("distrito_municipal") , nm_subprefeitura )

##### ZOE, ZEPAM e ZEP - áreas sem população #####
zon_temp <- le_WFS( 
                    "geoportal:zoneamento_2016_map1" , 
                    "&Filter=<Filter><Or><PropertyIsEqualTo><PropertyName>tx_zoneamento_perimetro</PropertyName><Literal>ZOE</Literal></PropertyIsEqualTo><PropertyIsEqualTo><PropertyName>tx_zoneamento_perimetro</PropertyName><Literal>ZEPAM</Literal></PropertyIsEqualTo><PropertyIsEqualTo><PropertyName>tx_zoneamento_perimetro</PropertyName><Literal>ZEP</Literal></PropertyIsEqualTo></Or></Filter>"
                    ) |> select( tx_zoneamento_perimetro )

IUPop_04_ZOE <- zon_temp |> filter( tx_zoneamento_perimetro == "ZOE" )
IUPop_05_ZEPAM <- zon_temp |> filter( tx_zoneamento_perimetro == "ZEPAM" )
IUPop_06_ZEP <- zon_temp |> filter( tx_zoneamento_perimetro == "ZEP" )

# -------------------------------------------------------------------------------------------------------------------- #
#### 10 - processamentos - associando população com macroárea ####
# -------------------------------------------------------------------------------------------------------------------- #

##### retirando ZOE, ZEPAM e ZEP das macroáreas, pois não há população ali #####
IUPop_11_exclusoes <- bind_rows( IUPop_04_ZOE , IUPop_05_ZEPAM , IUPop_06_ZEP ) |> st_cast( "POLYGON" )

IUPop_12_macroarea <- IUPop_02_macroarea |> 
  rmapshaper::ms_erase( IUPop_11_exclusoes )

IUPop_13_pop_macroarea_bruto <- st_intersection( IUPop_12_macroarea , IUPop_03_distrito  ) |>
  ##### área de sobreposição entre cada distrito e macroárea #####
  mutate( 
          area_ha_intersect = round(as.numeric(st_area(geometry)),5)
        ) |>
  st_drop_geometry() |>
  ##### área total distrito = soma dos pedaços cruzados com macroárea #####
  group_by( nm_distrito_municipal ) |>
  mutate( area_ha = sum( area_ha_intersect ) ) |>
  ungroup() |>
  ##### % de sobreposição entre cada distrito e macroárea #####
  mutate(
          area_ha_intersect_porcent = area_ha_intersect / area_ha
        ) |>
  arrange( sg_macro_divisao_pde , cd_distrito_municipal ) |>
  rename( distrito = nm_distrito_municipal ) |>
  ##### juntando população #####
  left_join( IUPop_01_popDistrito |> select(-c(codigo_distrito)) ) |>
  ##### esticando tabela #####
  pivot_longer( 
                cols = -c(sg_macro_divisao_pde:area_ha_intersect_porcent) , 
                names_to = c("Ano", "Gênero", "Idade"),
                names_pattern = "(.*)_(.*)_(.*)", 
                values_to = "População" 
              ) |>
  ##### calculando proporção #####
  mutate( População = População * area_ha_intersect_porcent ) |>
  ##### removendo colunas desnecessárias #####
  select( -contains( "area_ha" ) ) |>
  ##### renomeando nos padrões GEOINFO #####
  rename(
          SG_MACROAREA = sg_macro_divisao_pde,
          CD_DISTRITO = cd_distrito_municipal,
          NM_SUBPREF = nm_subprefeitura,
          SG_DISTRITO = ds_sigla,
          NM_DISTRITO = distrito,
          DT_ANO = Ano,
          TX_GENERO = Gênero,
          TX_FAIXAETARIA = Idade,
          QT_POPULACAO = População,
        ) |>
  ##### reordenando #####
  select(
          SG_MACROAREA,NM_SUBPREF,CD_DISTRITO,SG_DISTRITO,NM_DISTRITO,DT_ANO,TX_GENERO,TX_FAIXAETARIA,QT_POPULACAO
        )

##### salvando em CSV e RDS para consultas #####
IUPop_13_pop_macroarea_bruto |> write_csv2( "13_população-por-macroárea_2000-2050_bruto.csv" )
IUPop_13_pop_macroarea_bruto |> write_rds( "13_população-por-macroárea_2000-2050_bruto.rds" )
  
  
# -------------------------------------------------------------------------------------------------------------------- #
#### 20 - informações ####
# -------------------------------------------------------------------------------------------------------------------- #

###### 21 - população por macroárea ######
IUPop_21_pop_macroarea <- IUPop_13_pop_macroarea_bruto |>
  group_by( DT_ANO , SG_MACROAREA ) |>
  summarize( QT_POPULACAO = round(sum(QT_POPULACAO),0) ) |>
  ungroup() |>
  pivot_wider( names_from = SG_MACROAREA , values_from = QT_POPULACAO )
  
IUPop_21_pop_macroarea |> write_xlsx( "21_população-por-macroárea_2000-2050.xlsx" )

###### 22 - proporção da população da cidade por macroárea ######
pop2020 <- IUPop_13_pop_macroarea_bruto |>
  filter( DT_ANO == "2020" ) |> 
  summarize( sum(QT_POPULACAO) ) |>
  pull()

IUPop_13_pop_macroarea_bruto |> 
  filter( DT_ANO == "2020" ) |> 
  group_by( SG_MACROAREA ) |>
  summarise( `VL_POPULACAO_%_TOTAL` = sum( QT_POPULACAO )/pop2020 ) |>
  write_xlsx( "22_população-por-macroárea-proporcional.xlsx" )
