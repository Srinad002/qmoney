package com.crio.warmup.stock;

import com.crio.warmup.stock.dto.AnnualizedReturn;
import com.crio.warmup.stock.dto.Candle;
import com.crio.warmup.stock.dto.PortfolioTrade;
import com.crio.warmup.stock.dto.TiingoCandle;
import com.crio.warmup.stock.dto.TotalReturnsDto;
import com.crio.warmup.stock.log.UncaughtExceptionHandler;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.web.client.RestTemplate;


public class PortfolioManagerApplication {

  // 9ff2bf08ab1c8453cc8d60b1822122be09e4b5d7
  // 7dbe57448503290a356fd82845d34acbf726cb92
  private static final String token = "9ff2bf08ab1c8453cc8d60b1822122be09e4b5d7";

  public static String getToken() {
    return token;
  }

  public static List<String> mainReadFile(String[] args) throws IOException, URISyntaxException {
    List<PortfolioTrade> portfolioTrades = readTradesFromJson(args[0]);
    List<String> symbols = new ArrayList<>();
    for (PortfolioTrade portfolioTrade : portfolioTrades) {
      if (portfolioTrade.getSymbol() != null) {
        symbols.add(portfolioTrade.getSymbol());
      }
    }
    return symbols;
  }

  public static TotalReturnsDto createTotalReturnsDto(PortfolioTrade trade, LocalDate endDate,
      String token) {
    List<Candle> tiingoCandles = fetchCandles(trade, endDate, token);
    String symbol = trade.getSymbol();
    Double closePrice = getClosingPriceOnEndDate(tiingoCandles);
    TotalReturnsDto totalReturnsDto = new TotalReturnsDto(symbol, closePrice);
    return totalReturnsDto;
  }

  public static void sortTotalReturnDtos(List<TotalReturnsDto> totalReturnsDtos) {
    Collections.sort(totalReturnsDtos, new Comparator<TotalReturnsDto>() {
      @Override
      public int compare(TotalReturnsDto a, TotalReturnsDto b) {
        return Double.compare(a.getClosingPrice(), b.getClosingPrice());
      }
    });
  }
  
  public static List<String> getStocksFromTotalReturnDtos(List<TotalReturnsDto> totalReturnsDtos) {
    List<String> stocks = new ArrayList<>();
    for (TotalReturnsDto totalReturnsDto : totalReturnsDtos) {
      stocks.add(totalReturnsDto.getSymbol());
    }
    return stocks;
  }

  public static List<String> mainReadQuotes(String[] args) throws IOException, URISyntaxException {
    List<PortfolioTrade> portfolioTrades = readTradesFromJson(args[0]);
    List<TotalReturnsDto> totalReturnsDtos = new ArrayList<>();
    LocalDate endDate = LocalDate.parse(args[1]);
    for (PortfolioTrade trade : portfolioTrades) {
      TotalReturnsDto totalReturnsDto = createTotalReturnsDto(trade, endDate, token);
      totalReturnsDtos.add(totalReturnsDto);
    }
    sortTotalReturnDtos(totalReturnsDtos);
    return getStocksFromTotalReturnDtos(totalReturnsDtos);
  }

  public static List<PortfolioTrade> readTradesFromJson(String filename)
      throws IOException, URISyntaxException {
    ObjectMapper objectMapper = getObjectMapper();
    File input = resolveFileFromResources(filename);
    List<PortfolioTrade> portfolioTrades =
        objectMapper.readValue(input, new TypeReference<List<PortfolioTrade>>() {});
    return portfolioTrades;
  }

  public static String prepareUrl(PortfolioTrade trade, LocalDate endDate, String token) {
    String url = "https://api.tiingo.com/tiingo/daily/" + trade.getSymbol() + "/prices?startDate="
        + trade.getPurchaseDate() + "&endDate=" + endDate + "&token=" + token;
    return url;
  }

  // TODO: CRIO_TASK_MODULE_CALCULATIONS
  // Now that you have the list of PortfolioTrade and their data, calculate annualized returns
  // for the stocks provided in the Json.
  // Use the function you just wrote #calculateAnnualizedReturns.
  // Return the list of AnnualizedReturns sorted by annualizedReturns in descending order.

  // Note:
  // 1. You may need to copy relevant code from #mainReadQuotes to parse the Json.
  // 2. Remember to get the latest quotes from Tiingo API.

  // TODO:
  // Ensure all tests are passing using below command
  // ./gradlew test --tests ModuleThreeRefactorTest
  static Double getOpeningPriceOnStartDate(List<Candle> candles) {
    return candles.get(0).getOpen();
  }

  public static Double getClosingPriceOnEndDate(List<Candle> candles) {
    return candles.get(candles.size() - 1).getClose();
  }

  public static LocalDate getEndLocalDate(List<Candle> candles) {
    return candles.get(candles.size() - 1).getDate();
  }

  public static List<Candle> fetchCandles(PortfolioTrade trade, LocalDate endDate, String token) {
    RestTemplate restTemplate = new RestTemplate();
    String url = prepareUrl(trade, endDate, token);
    Candle[] candles = restTemplate.getForObject(url, TiingoCandle[].class);
    if (candles == null) {
      return Collections.emptyList();
    }
    return Arrays.asList(candles);
  }

  public static List<AnnualizedReturn> mainCalculateSingleReturn(String[] args)
      throws IOException, URISyntaxException {
    List<AnnualizedReturn> annualizedReturns = new ArrayList<>();
    List<PortfolioTrade> portfolioTrades = readTradesFromJson(args[0]);
    LocalDate givenEndDate = LocalDate.parse(args[1]);

    for (PortfolioTrade portfolioTrade : portfolioTrades) {
      List<Candle> candles = fetchCandles(portfolioTrade, givenEndDate, token);
      LocalDate endDate = getEndLocalDate(candles);
      Double buyPrice = getOpeningPriceOnStartDate(candles);
      Double sellPrice = getClosingPriceOnEndDate(candles);
      AnnualizedReturn annualizedReturn =
          calculateAnnualizedReturns(endDate, portfolioTrade, buyPrice, sellPrice);
      annualizedReturns.add(annualizedReturn);
    }
    sortAnnualizedReturns(annualizedReturns);
    return annualizedReturns;
  }
  
  public static List<AnnualizedReturn> sortAnnualizedReturns(
      List<AnnualizedReturn> annualizedReturns) {
    Collections.sort(annualizedReturns, new Comparator<AnnualizedReturn>() {

      @Override
      public int compare(AnnualizedReturn a, AnnualizedReturn b) {
        return Double.compare(b.getAnnualizedReturn(), a.getAnnualizedReturn());
      }
      
    });
    return annualizedReturns;    
  }

  // TODO: CRIO_TASK_MODULE_CALCULATIONS
  // Return the populated list of AnnualizedReturn for all stocks.
  // Annualized returns should be calculated in two steps:
  // 1. Calculate totalReturn = (sell_value - buy_value) / buy_value.
  // 1.1 Store the same as totalReturns
  // 2. Calculate extrapolated annualized returns by scaling the same in years span.
  // The formula is:
  // annualized_returns = (1 + total_returns) ^ (1 / total_num_years) - 1
  // 2.1 Store the same as annualized_returns
  // Test the same using below specified command. The build should be successful.
  // ./gradlew test --tests PortfolioManagerApplicationTest.testCalculateAnnualizedReturn

  public static AnnualizedReturn calculateAnnualizedReturns(LocalDate endDate, PortfolioTrade trade,
      Double buyPrice, Double sellPrice) {
    Double totalNoOfYears =
        ((double) ChronoUnit.DAYS.between(trade.getPurchaseDate(), endDate)) / 365.24;    
    Double totalReturns = (sellPrice - buyPrice) / buyPrice;
    String symbol = trade.getSymbol();
    Double annualizedReturns = Math.pow((1.0 + totalReturns), 1.0 / totalNoOfYears) - 1;
    return new AnnualizedReturn(symbol, annualizedReturns, totalReturns);
  }

  private static void printJsonObject(Object object) throws IOException {
    Logger logger = Logger.getLogger(PortfolioManagerApplication.class.getCanonicalName());
    ObjectMapper mapper = new ObjectMapper();
    logger.info(mapper.writeValueAsString(object));
  }

  private static File resolveFileFromResources(String filename) throws URISyntaxException {
    return Paths.get(Thread.currentThread().getContextClassLoader().getResource(filename).toURI())
        .toFile();
  }

  private static ObjectMapper getObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());
    return objectMapper;
  }

  public static List<String> debugOutputs() {

    String valueOfArgument0 = "trades.json";
    String resultOfResolveFilePathArgs0 =
        "/home/crio-user/workspace/virivadaprudhvisrinadh-ME_QMONEY_V2/qmoney/bin/main/trades.json";
    String toStringOfObjectMapper = "com.fasterxml.jackson.databind.ObjectMapper@67c27493";
    String functionNameFromTestFileInStackTrace = "PortfolioManagerApplicationTest.mainReadFile()";
    String lineNumberFromTestFileInStackTrace = "29";

    return Arrays.asList(
        new String[] {valueOfArgument0, resultOfResolveFilePathArgs0, toStringOfObjectMapper,
            functionNameFromTestFileInStackTrace, lineNumberFromTestFileInStackTrace});
  }

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler());
    ThreadContext.put("runId", UUID.randomUUID().toString());
    printJsonObject(mainCalculateSingleReturn(args));
  }
}
