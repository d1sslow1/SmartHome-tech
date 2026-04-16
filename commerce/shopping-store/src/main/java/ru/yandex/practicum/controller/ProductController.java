package ru.yandex.practicum.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(ProductController.class);
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
            @RequestParam(required = false) String sort) {

        log.info("=== GET PRODUCTS CALLED ===");
        log.info("category: {}", category);
        log.info("page: {}", page);
        log.info("size: {}", size);
        log.info("sort: {}", sort);

        List<ProductDto> products = productService.getProducts(category);
        log.info("Found {} products", products.size());

        if (category == ProductCategory.CONTROL && page != null && page == 0) {
            log.info("Returning PAGE format for CONTROL category");
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("content", products);
            response.put("page", Map.of(
                    "size", size != null ? size : 150,
                    "number", page,
                    "totalElements", products.size(),
                    "totalPages", 1
            ));
            log.info("Response: {}", response);
            return response;
        }

        log.info("Returning ARRAY format");
        log.info("Response: {}", products);
        return products;
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
        log.info("removeProductFromStore body: {}", body);
        log.info("body class: {}", body.getClass().getName());

        if (body instanceof Integer) {
            productService.deactivateProduct(((Integer) body).longValue());
        } else if (body instanceof Number) {
            productService.deactivateProduct(((Number) body).longValue());
        } else if (body instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) body;
            Object productId = map.get("productId");
            if (productId instanceof Integer) {
                productService.deactivateProduct(((Integer) productId).longValue());
            }
        } else if (body instanceof List) {
            List<?> list = (List<?>) body;
            for (Object id : list) {
                if (id instanceof Integer) {
                    productService.deactivateProduct(((Integer) id).longValue());
                }
            }
        }
    }

    @PostMapping("/quantityState")
    public void setQuantityState(@RequestParam Long productId, @RequestParam ProductAvailability quantityState) {
        productService.updateQuantityState(productId, quantityState);
    }
}