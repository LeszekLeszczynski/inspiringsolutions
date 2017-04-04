#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#

load("klient_stats_woj.RData")
load("klient_stats_pow.RData")

load("wojs.RData")
load("powiaty.RData")
load("PL_zip.RData")
load("raport_transakcje_total.RData")

library(shiny)
library(sparklyr)
library(dplyr)
library(DBI)
library(markdown)
library(plotly)
library(ggthemes)
library(scales)
library(rgdal)
library(leaflet)

Sys.setenv(SPARK_HOME="/usr/hdp/current/spark-client")
# connect to local cluster
sc <- spark_connect(master = "yarn-client")

invoke(hive_context(sc), "sql", "USE bzwbk")

# Define UI for application that draws a histogram
ui <- navbarPage("BZWBK",
                 tabPanel("Table",
                          sidebarLayout(
                            sidebarPanel(
                              sliderInput("gosp",
                                          "Liczba osob gosp:",
                                          min = 1,
                                          max = 5,
                                          value = 1)
                            )
                            ,
                            mainPanel(
                              tableOutput("table")
                            )
                          )
                 )
                 ,
                 tabPanel("Profil klienta - mapy",
                          sidebarLayout(
                            sidebarPanel(
                              selectInput(inputId = h3("Okres"),
                                          label = "Okres:",
                                          choices = c("1601", "1602", "1603", "1604", "1605", "1606", "1607", "1608", "1609", "1610", "1611", "1612"),
                                          selected = "1601"),

                              radioButtons("poziom", label = h3("Poziom"),
                                           choices = list("województwa" = "1", 
                                                          "powiaty" = "2"
                                           ),
                                           selected = 1),
                              
                              radioButtons("statystyka", label = h3("Statystyka"),
                                           choices = list("Liczba klientów" = "1", 
                                                          "Średni wiek" = "2",
                                                          "Średni staż" = "3",
                                                          "Dystrybucja rachunków bieżących" = "4",
                                                          "Dystrybucja rachunków k_kred" = "5"
                                           ),
                                           selected = 1)
                            )
                            ,
                            mainPanel(
                              leafletOutput("nowa_mapa")
                            )
                          )
                 )
                 ,
                 tabPanel("Transakcje",
                          mainPanel(
                            plotlyOutput("transakcje")
                          )
                 )
)

# Define server logic required to draw a histogram
server <- function(input, output) {
  
  output$table <- renderTable({
    data <- tbl(sc, 'klient') %>% dplyr:: filter(liczba_osob_gosp < input$gosp) %>% group_by(wyksztalcenie) %>% summarise(count=n()) %>% collect()
  })
  
  # output$mapy <- renderPlot({
  #   
  #   stat = switch(input$statystyka,
  #                 "1" = "count_all",
  #                 "2" = "wiek",
  #                 "3" = "staz",
  #                 "4" = "dystr_rach_biez",
  #                 "5" = "dystr_rach_k_kred")
  #   
  #   map <- ggplot() +
  #     geom_polygon(data = plotData2, 
  #                  aes(long, lat, group = woj, 
  #                      fill = get(stat)),
  #                  colour = "white", lwd=0, alpha=0.8) +
  #     labs(x = "E", y = "N", fill = "Liczba\nklientw")
  #   
  #   map + theme_tufte() + theme_map() + scale_fill_distiller("Skala", palette = "Greens", breaks = pretty_breaks(n = 12),
  #                                                            trans = "reverse")
  # },     height = 500, width = 700)
  
    output$nowa_mapa <- renderLeaflet({
      
      wybrany_okres = "1601"
      
      stat = switch(input$statystyka,
                    "1" = "count_all",
                    "2" = "wiek",
                    "3" = "staz",
                    "4" = "dystr_rach_biez",
                    "5" = "dystr_rach_k_kred")
 
      ## WOJEWÓDZTWA
      if (input$poziom=="1") {
      
      klient_stats_woj_filter <- dplyr:: filter(klient_stats_woj,okres==wybrany_okres)
      wojs@data = data.frame(wojs@data, klient_stats_woj_filter[match(wojs@data$woj, klient_stats_woj_filter$woj),])
      
      labels_woj <- sprintf(
        "<strong>%s</strong><br/>
        klienci: %g <sup></sup><br/>
        wiek: %g <sup></sup><br/>
        staż: %g <sup></sup><br/>
        rachunki biezace: %g <sup></sup><br/>
        rachunki k_kred: %g <sup></sup>",
        
        wojs@data$VARNAME_1, 
        wojs@data$count_all,
        wojs@data$wiek, 
        wojs@data$staz,
        wojs@data$dystr_rach_biez,
        wojs@data$dystr_rach_k_kred
      ) %>% lapply(htmltools::HTML)
      
      
      leaflet(wojs) %>%
        addPolygons(color = "#444444", weight = 1, smoothFactor = 0.5,
                    opacity = 1.0, fillOpacity = 0.5,
                    fillColor = ~colorQuantile("YlOrRd", get(stat))(get(stat)),
                    highlightOptions = highlightOptions(color = "white", weight = 2, bringToFront = TRUE),
                    label = labels_woj,
                    labelOptions = labelOptions(
                      style = list("font-weight" = "normal", padding = "3px 8px"),
                      textsize = "15px",
                      direction = "auto")
        ) %>%
        addTiles()
      
      } else {
      ## POWIATY
      
      klient_stats_pow_filter <- dplyr:: filter(klient_stats_pow,okres==wybrany_okres)
      powiaty@data = data.frame(powiaty@data, klient_stats_pow_filter[match(powiaty@data$powiat, klient_stats_pow_filter$powiat),])
      
      labels_pow <- sprintf(
        "<strong>%s</strong><br/>
        klienci: %g <sup></sup><br/>
        wiek: %g <sup></sup><br/>
        staż: %g <sup></sup><br/>
        rachunki biezace: %g <sup></sup><br/>
        rachunki k_kred: %g <sup></sup>",
        
        powiaty@data$VARNAME_2, 
        powiaty@data$count_all,
        powiaty@data$wiek, 
        powiaty@data$staz,
        powiaty@data$dystr_rach_biez,
        powiaty@data$dystr_rach_k_kred
      ) %>% lapply(htmltools::HTML)
      
      leaflet(powiaty) %>%
        addPolygons(color = "#444444", weight = 1, smoothFactor = 0.5,
                    opacity = 1.0, fillOpacity = 0.5,
                    fillColor = ~colorQuantile("YlOrRd", get(stat))(get(stat)),
                    highlightOptions = highlightOptions(color = "white", weight = 2, bringToFront = TRUE),
                    label = labels_pow,
                    labelOptions = labelOptions(
                      style = list("font-weight" = "normal", padding = "3px 8px"),
                      textsize = "15px",
                      direction = "auto")
        ) %>% 
        addTiles()
      }
      
    })

  output$transakcje <- renderPlotly({

    plot_ly(data=raport_transakcje_total, x  = ~data, y = ~PLN, name = 'PLN', type = 'scatter', mode = 'lines', line = list(color = 'rgb(205, 12, 24)', width = 4)) %>% 
      add_trace(y = ~EUR, name = 'EUR', line = list(color = 'rgb(166,206,227)', width = 4)) %>%
      add_trace(y = ~USD, name = 'USD', line = list(color = 'rgb(22, 96, 167)', width = 4)) %>%
      add_trace(y = ~GBP, name = 'GBP', line = list(color = 'rgb(251,154,153)', width = 4)) %>%
      add_trace(y = ~CHF, name = 'CHF', line = list(color = 'rgb(227,26,28)', width = 4)) %>%
      add_trace(y = ~NOK, name = 'NOK', line = list(color = 'rgb(51,160,44)', width = 4)) %>%
      add_trace(y = ~AUD, name = 'AUD', line = list(color = 'rgb(254,224,144)', width = 4)) %>%
      add_trace(y = ~CAD, name = 'CAD', line = list(color = 'rgb(171,217,233)', width = 4)) %>%
      add_trace(y = ~CZK, name = 'CZK', line = list(color = 'rgb(69,117,180)', width = 4)) %>%
      add_trace(y = ~DKK, name = 'DKK', line = list(color = 'rgb(49,54,149)', width = 4)) %>%
      add_trace(y = ~JPY, name = 'JPY', line = list(color = 'rgb(116,173,20)', width = 4)) %>%
      add_trace(y = ~SEK, name = 'SEK', line = list(color = 'rgb(253,174,97)', width = 4)) %>%
      layout(yaxis = list(title = "total"))

  })
  
}

# Run the application 
shinyApp(ui = ui, server = server)

