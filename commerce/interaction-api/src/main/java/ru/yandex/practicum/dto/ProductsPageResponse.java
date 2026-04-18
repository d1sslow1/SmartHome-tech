package ru.yandex.practicum.dto;

import java.util.List;
import java.util.Map;

public class ProductsPageResponse {
    private List<ProductDto> content;
    private Map<String, Object> page;
    private Map<String, Boolean> sort;

    public ProductsPageResponse() {}

    public ProductsPageResponse(List<ProductDto> content, int page, int size, long totalElements, int totalPages, boolean sorted) {
        this.content = content;
        this.page = Map.of(
                "size", size,
                "number", page,
                "totalElements", totalElements,
                "totalPages", totalPages
        );
        this.sort = Map.of(
                "sorted", sorted,
                "unsorted", !sorted,
                "empty", false
        );
    }

    public List<ProductDto> getContent() { return content; }
    public void setContent(List<ProductDto> content) { this.content = content; }

    public Map<String, Object> getPage() { return page; }
    public void setPage(Map<String, Object> page) { this.page = page; }

    public Map<String, Boolean> getSort() { return sort; }
    public void setSort(Map<String, Boolean> sort) { this.sort = sort; }
}