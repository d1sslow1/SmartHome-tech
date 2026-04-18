package ru.yandex.practicum.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public class ProductsPageResponse {

    private List<ProductDto> content;

    @JsonProperty("page")
    private Map<String, Object> pageMetadata;

    @JsonProperty("sort")
    private Map<String, Boolean> sortMetadata;

    private long totalElements;
    private int totalPages;
    private boolean first;
    private boolean last;
    private int size;
    private int number;
    private int numberOfElements;
    private boolean empty;

    public ProductsPageResponse() {}

    public ProductsPageResponse(List<ProductDto> content, int page, int size,
                                long totalElements, int totalPages, boolean sorted) {
        this.content = content;
        this.totalElements = totalElements;
        this.totalPages = totalPages;
        this.first = (page == 0);
        this.last = (page >= totalPages - 1);
        this.size = size;
        this.number = page;
        this.numberOfElements = content != null ? content.size() : 0;
        this.empty = content == null || content.isEmpty();

        this.pageMetadata = Map.of(
                "size", size,
                "number", page,
                "totalElements", totalElements,
                "totalPages", totalPages
        );

        this.sortMetadata = Map.of(
                "sorted", sorted,
                "unsorted", !sorted,
                "empty", false
        );
    }

    public List<ProductDto> getContent() { return content; }
    public void setContent(List<ProductDto> content) { this.content = content; }

    public Map<String, Object> getPage() { return pageMetadata; }
    public void setPage(Map<String, Object> page) { this.pageMetadata = page; }

    public Map<String, Boolean> getSort() { return sortMetadata; }
    public void setSort(Map<String, Boolean> sort) { this.sortMetadata = sort; }

    public long getTotalElements() { return totalElements; }
    public void setTotalElements(long totalElements) { this.totalElements = totalElements; }

    public int getTotalPages() { return totalPages; }
    public void setTotalPages(int totalPages) { this.totalPages = totalPages; }

    public boolean isFirst() { return first; }
    public void setFirst(boolean first) { this.first = first; }

    public boolean isLast() { return last; }
    public void setLast(boolean last) { this.last = last; }

    public int getSize() { return size; }
    public void setSize(int size) { this.size = size; }

    public int getNumber() { return number; }
    public void setNumber(int number) { this.number = number; }

    public int getNumberOfElements() { return numberOfElements; }
    public void setNumberOfElements(int numberOfElements) { this.numberOfElements = numberOfElements; }

    public boolean isEmpty() { return empty; }
    public void setEmpty(boolean empty) { this.empty = empty; }
}