package ru.yandex.practicum.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public class ProductsPageResponse {

    private List<ProductDto> content;

    @JsonProperty("page")
    private Map<String, Object> pageMetadata;

    private SortObject[] sort;

    private long totalElements;
    private int totalPages;
    private boolean first;
    private boolean last;
    private int size;
    private int number;
    private int numberOfElements;
    private boolean empty;

    private PageableObject pageable;

    public ProductsPageResponse() {}

    public ProductsPageResponse(List<ProductDto> content, int page, int size,
                                long totalElements, int totalPages, boolean sorted, String direction) {
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

        this.sort = new SortObject[] { new SortObject(sorted, direction) };
        this.pageable = new PageableObject(page, size, sorted, direction);
    }

    public List<ProductDto> getContent() { return content; }
    public void setContent(List<ProductDto> content) { this.content = content; }

    public Map<String, Object> getPage() { return pageMetadata; }
    public void setPage(Map<String, Object> page) { this.pageMetadata = page; }

    public SortObject[] getSort() { return sort; }
    public void setSort(SortObject[] sort) { this.sort = sort; }

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

    public PageableObject getPageable() { return pageable; }
    public void setPageable(PageableObject pageable) { this.pageable = pageable; }

    public static class SortObject {
        private boolean sorted;
        private boolean unsorted;
        private boolean empty;
        private String direction;

        public SortObject() {}

        public SortObject(boolean sorted, String direction) {
            this.sorted = sorted;
            this.unsorted = !sorted;
            this.empty = false;
            this.direction = direction;
        }

        public boolean isSorted() { return sorted; }
        public void setSorted(boolean sorted) { this.sorted = sorted; }

        public boolean isUnsorted() { return unsorted; }
        public void setUnsorted(boolean unsorted) { this.unsorted = unsorted; }

        public boolean isEmpty() { return empty; }
        public void setEmpty(boolean empty) { this.empty = empty; }

        public String getDirection() { return direction; }
        public void setDirection(String direction) { this.direction = direction; }
    }

    public static class PageableObject {
        private int pageNumber;
        private int pageSize;
        private SortObject sort;
        private boolean paged;
        private boolean unpaged;
        private long offset;

        public PageableObject() {}

        public PageableObject(int pageNumber, int pageSize, boolean sorted, String direction) {
            this.pageNumber = pageNumber;
            this.pageSize = pageSize;
            this.sort = new SortObject(sorted, direction);
            this.paged = true;
            this.unpaged = false;
            this.offset = (long) pageNumber * pageSize;
        }

        public int getPageNumber() { return pageNumber; }
        public void setPageNumber(int pageNumber) { this.pageNumber = pageNumber; }

        public int getPageSize() { return pageSize; }
        public void setPageSize(int pageSize) { this.pageSize = pageSize; }

        public SortObject getSort() { return sort; }
        public void setSort(SortObject sort) { this.sort = sort; }

        public boolean isPaged() { return paged; }
        public void setPaged(boolean paged) { this.paged = paged; }

        public boolean isUnpaged() { return unpaged; }
        public void setUnpaged(boolean unpaged) { this.unpaged = unpaged; }

        public long getOffset() { return offset; }
        public void setOffset(long offset) { this.offset = offset; }
    }
}