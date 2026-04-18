package ru.yandex.practicum.controller;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.dto.ProductsPageResponse;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.service.ProductService;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/shopping-store")
public class ProductController {

    private final ProductService productService;

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @GetMapping("/{id}")
    public ProductDto getProduct(@PathVariable Long id) {
        return productService.getProduct(id);
    }

    @GetMapping
    public Object getProducts(
            @RequestParam(required = false) ProductCategory category,
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer size,
            @RequestParam(required = false) String[] sort) {

        if (page == null) {
            return productService.getProductsList(category);
        }

        Sort sorting = Sort.unsorted();
        if (sort != null && sort.length > 0) {
            String[] sortParts = sort[0].split(",");
            String property = sortParts[0];
            Sort.Direction direction = Sort.Direction.ASC;
            if (sortParts.length > 1 && "DESC".equalsIgnoreCase(sortParts[1])) {
                direction = Sort.Direction.DESC;
            }
            sorting = Sort.by(direction, property);
        }

        Pageable pageable = PageRequest.of(page, size != null ? size : 20, sorting);
        Page<ProductDto> productPage = productService.getProductsPage(category, pageable);

        return new ProductsPageResponse(
                productPage.getContent(),
                productPage.getNumber(),
                productPage.getSize(),
                productPage.getTotalElements(),
                productPage.getTotalPages(),
                sorting.isSorted()
        );
    }

    @PutMapping
    public ProductDto addOrUpdateProduct(@RequestBody ProductDto product) {
        if (product.getId() == null) {
            return productService.addProduct(product);
        } else {
            return productService.updateProduct(product);
        }
    }

    @PostMapping
    public ProductDto addProductPost(@RequestBody ProductDto product) {
        return productService.addProduct(product);
    }

    @PostMapping("/removeProductFromStore")
    public boolean removeProductFromStore(@RequestBody Object body) {
        Long productId = extractProductId(body);
        if (productId != null) {
            productService.deactivateProduct(productId);
            return true;
        }
        return false;
    }

    @PostMapping("/quantityState")
    public boolean setQuantityState(@RequestBody SetQuantityStateRequest request) {
        productService.updateQuantityState(request.getProductId(), request.getQuantityState());
        return true;
    }

    private Long extractProductId(Object body) {
        if (body instanceof Integer) {
            return ((Integer) body).longValue();
        } else if (body instanceof Number) {
            return ((Number) body).longValue();
        } else if (body instanceof String) {
            try {
                return Long.parseLong((String) body);
            } catch (NumberFormatException e) {
                return null;
            }
        } else if (body instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) body;
            Object productId = map.get("productId");
            if (productId instanceof Integer) {
                return ((Integer) productId).longValue();
            } else if (productId instanceof Number) {
                return ((Number) productId).longValue();
            } else if (productId instanceof String) {
                try {
                    return Long.parseLong((String) productId);
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        }
        return null;
    }

    static class SetQuantityStateRequest {
        private Long productId;
        private ProductAvailability quantityState;

        public Long getProductId() { return productId; }
        public void setProductId(Long productId) { this.productId = productId; }

        public ProductAvailability getQuantityState() { return quantityState; }
        public void setQuantityState(ProductAvailability quantityState) { this.quantityState = quantityState; }
    }
}