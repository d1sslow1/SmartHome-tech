package ru.yandex.practicum.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(ProductController.class);
    private final ProductService productService;
    private final ObjectMapper objectMapper = new ObjectMapper();

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
        log.info("category: {}, page: {}, size: {}, sort: {}", category, page, size, sort);

        // Если есть параметр page - возвращаем объект с content
        if (page != null) {
            String sortField = "productName";
            Sort.Direction direction = Sort.Direction.ASC;
            boolean sorted = false;

            if (sort != null) {
                String[] sortParts = sort.split(",");
                sortField = sortParts[0];
                direction = Sort.Direction.fromString(sortParts[1]);
                sorted = true;
            }

            Pageable pageable = PageRequest.of(page, size != null ? size : 150, Sort.by(direction, sortField));
            Page<ProductDto> productPage = productService.getProductsPage(category, pageable);

            ProductsPageResponse response = new ProductsPageResponse(
                    productPage.getContent(),
                    productPage.getNumber(),
                    productPage.getSize(),
                    productPage.getTotalElements(),
                    productPage.getTotalPages(),
                    sorted
            );

            // ЛОГИРУЕМ ОТВЕТ
            try {
                String json = objectMapper.writeValueAsString(response);
                log.info("RESPONSE: {}", json);
            } catch (Exception e) {
                log.error("Failed to serialize response", e);
            }

            return response;
        }

        // Иначе возвращаем простой массив
        List<ProductDto> products = productService.getProductsList(category);

        // ЛОГИРУЕМ ОТВЕТ
        try {
            String json = objectMapper.writeValueAsString(products);
            log.info("RESPONSE (array): {}", json);
        } catch (Exception e) {
            log.error("Failed to serialize response", e);
        }

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