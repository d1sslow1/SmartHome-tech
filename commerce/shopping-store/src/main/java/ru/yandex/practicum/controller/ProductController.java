package ru.yandex.practicum.controller;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.service.ProductService;

import java.util.LinkedHashMap;
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
    public Map<String, Object> getProducts(
            @RequestParam(required = false) ProductCategory category,
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer size,
            @RequestParam(required = false) String sort) {

        List<ProductDto> products;

        if (page != null) {
            // С пагинацией - берём из Page
            String sortField = "productName";
            Sort.Direction direction = Sort.Direction.ASC;
            if (sort != null) {
                String[] sortParts = sort.split(",");
                sortField = sortParts[0];
                direction = Sort.Direction.fromString(sortParts[1]);
            }
            Pageable pageable = PageRequest.of(page, size != null ? size : 150, Sort.by(direction, sortField));
            Page<ProductDto> productPage = productService.getProductsPage(category, pageable);
            products = productPage.getContent();
        } else {
            // Без пагинации - берём List
            products = productService.getProductsList(category);
        }

        // ВСЕГДА возвращаем объект с content!
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("content", products);
        response.put("page", Map.of(
                "size", size != null ? size : 150,
                "number", page != null ? page : 0,
                "totalElements", products.size(),
                "totalPages", 1
        ));
        return response;
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
    public void removeProductFromStore(@RequestBody Object body) {
        if (body instanceof Integer) {
            productService.deactivateProduct(((Integer) body).longValue());
        } else if (body instanceof Number) {
            productService.deactivateProduct(((Number) body).longValue());
        } else if (body instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) body;
            Object productId = map.get("productId");
            if (productId instanceof Integer) {
                productService.deactivateProduct(((Integer) productId).longValue());
            } else if (productId instanceof Number) {
                productService.deactivateProduct(((Number) productId).longValue());
            }
        }
    }

    @PostMapping("/quantityState")
    public void setQuantityState(@RequestParam Long productId, @RequestParam ProductAvailability quantityState) {
        productService.updateQuantityState(productId, quantityState);
    }
}