package model;

import java.math.BigDecimal;

public class Venda {

  private Long operacao;
  private Long cliente;
  private Integer quantidadeIngresso;
  private BigDecimal valorTotal;
  private String status;

  public Venda() {

  }

  public Venda(Long operacao, Long cliente, Integer quantidadeIngresso, BigDecimal valorTotal) {
    this.operacao = operacao;
    this.cliente = cliente;
    this.quantidadeIngresso = quantidadeIngresso;
    this.valorTotal = valorTotal;
  }

  public Long getOperacao() {
    return operacao;
  }

  public void setOperacao(Long operacao) {
    this.operacao = operacao;
  }

  public Long getCliente() {
    return cliente;
  }

  public void setCliente(Long cliente) {
    this.cliente = cliente;
  }

  public Integer getQuantidadeIngresso() {
    return quantidadeIngresso;
  }

  public void setQuantidadeIngresso(Integer quantidadeIngresso) {
    this.quantidadeIngresso = quantidadeIngresso;
  }

  public BigDecimal getValorTotal() {
    return valorTotal;
  }

  public void setValorTotal(BigDecimal valorTotal) {
    this.valorTotal = valorTotal;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  @Override
  public String toString() {
    return "Venda{" + "operacao=" + operacao + ", cliente=" + cliente + ", quantidadeIngresso=" + quantidadeIngresso + ", valorTotal=" + valorTotal
        + ", status='" + status + '\'' + '}';
  }
}

