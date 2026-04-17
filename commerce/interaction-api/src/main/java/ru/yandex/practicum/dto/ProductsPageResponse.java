package ru.yandex.practicum.dto;

import java.util.List;
import java.util.Map;

public class ProductsPageResponse {
    private List<ProductDto> content;
    private Map<String, Object> page;

    public ProductsPageResponse() {}

    public ProductsPageResponse(List<ProductDto> content, int page, int size, long totalElements, int totalPages) {
        this.content = content;
        this.page = Map.of(
                "size", size,
                "number", page,
                "totalElements", totalElements,
                "totalPages", totalPages
        );
    }

    public List<ProductDto> getContent() {
        return content;
    }

    public void setContent(List<ProductDto> content) {
        this.content = content;
    }

    public Map<String, Object> getPage() {
        return page;
    }

    public void setPage(Map<String, Object> page) {
        this.page = page;
    }
}